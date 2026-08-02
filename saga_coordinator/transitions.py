"""
The saga's transition table, expressed explicitly rather than via a state
machine library. Each entry says: "if the saga is currently in state X and
event Y arrives, move to state Z and emit command W". Writing it this way
(instead of scattering the logic across a class hierarchy) makes the whole
lifecycle readable as a single table, which matters a lot for something
whose entire job is being an auditable source of truth for a workflow.

              OrderCreated
                   |
                   v
              RESERVING  --InventoryReservationFailed--> CANCELLED (+CancelOrder)
                   |
        InventoryReserved
                   v
              CHARGING  ------PaymentFailed------> COMPENSATING (+ReleaseInventory)
                   |                                      |
           PaymentSucceeded                       InventoryReleased
                   v                                      v
              CONFIRMED (+ConfirmOrder)              CANCELLED (+CancelOrder)

GUARD CLAUSES: every transition below checks that the saga is in the
EXPECTED current state before applying itself. If it isn't (e.g. a
redelivered/duplicate event arrives after the saga has already moved past
that point), the handler logs a warning and does nothing -- this is what
makes the coordinator safe under RabbitMQ's at-least-once delivery without
needing a separate idempotency store like Payment Service uses (the state
column itself IS the idempotency guard here).
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from saga_coordinator.models import OutboxMessage
from saga_coordinator.models import SagaState as SagaStateRow
from shared.events import (
    BaseMessage,
    CancelOrder,
    ChargePayment,
    ConfirmOrder,
    InventoryReleased,
    InventoryReservationFailed,
    InventoryReserved,
    OrderCreated,
    PaymentFailed,
    PaymentSucceeded,
    ReleaseInventory,
    ReserveInventory,
)
from shared.events import SagaState as SagaStateEnum


@dataclass
class TransitionOutcome:
    applied: bool
    reason: str | None = None  # set when applied=False, for logging


def _emit(session: AsyncSession, message: BaseMessage) -> None:
    session.add(OutboxMessage(routing_key=message.routing_key, payload=message.model_dump(mode="json")))


async def _get_saga(session: AsyncSession, order_id: str) -> SagaStateRow | None:
    result = await session.execute(select(SagaStateRow).where(SagaStateRow.order_id == order_id))
    return result.scalar_one_or_none()


async def handle_order_created(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = OrderCreated.model_validate(payload)

    existing = await _get_saga(session, event.order_id)
    if existing is not None:
        # Duplicate OrderCreated delivery -- saga already exists, do nothing.
        return TransitionOutcome(applied=False, reason="saga_already_exists")

    saga = SagaStateRow(
        order_id=event.order_id,
        state=SagaStateEnum.RESERVING,
        sku=event.sku,
        qty=event.qty,
        user_id=event.user_id,
        amount_cents=event.amount_cents,
    )
    session.add(saga)
    _emit(session, ReserveInventory(order_id=event.order_id, sku=event.sku, qty=event.qty))
    return TransitionOutcome(applied=True)


async def handle_inventory_reserved(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = InventoryReserved.model_validate(payload)
    saga = await _get_saga(session, event.order_id)

    if saga is None:
        return TransitionOutcome(applied=False, reason="unknown_saga")
    if saga.state != SagaStateEnum.RESERVING:
        return TransitionOutcome(applied=False, reason=f"unexpected_state:{saga.state}")

    saga.state = SagaStateEnum.CHARGING
    _emit(session, ChargePayment(order_id=event.order_id, user_id=saga.user_id, amount_cents=saga.amount_cents))
    return TransitionOutcome(applied=True)


async def handle_inventory_reservation_failed(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = InventoryReservationFailed.model_validate(payload)
    saga = await _get_saga(session, event.order_id)

    if saga is None:
        return TransitionOutcome(applied=False, reason="unknown_saga")
    if saga.state != SagaStateEnum.RESERVING:
        return TransitionOutcome(applied=False, reason=f"unexpected_state:{saga.state}")

    saga.state = SagaStateEnum.CANCELLED
    saga.failure_reason = event.reason
    _emit(session, CancelOrder(order_id=event.order_id, reason=f"inventory_unavailable:{event.reason}"))
    return TransitionOutcome(applied=True)


async def handle_payment_succeeded(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = PaymentSucceeded.model_validate(payload)
    saga = await _get_saga(session, event.order_id)

    if saga is None:
        return TransitionOutcome(applied=False, reason="unknown_saga")
    if saga.state != SagaStateEnum.CHARGING:
        return TransitionOutcome(applied=False, reason=f"unexpected_state:{saga.state}")

    saga.state = SagaStateEnum.CONFIRMED
    saga.payment_id = event.payment_id
    _emit(session, ConfirmOrder(order_id=event.order_id))
    return TransitionOutcome(applied=True)


async def handle_payment_failed(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = PaymentFailed.model_validate(payload)
    saga = await _get_saga(session, event.order_id)

    if saga is None:
        return TransitionOutcome(applied=False, reason="unknown_saga")
    if saga.state != SagaStateEnum.CHARGING:
        return TransitionOutcome(applied=False, reason=f"unexpected_state:{saga.state}")

    # COMPENSATION PATH: payment declined after inventory was already
    # reserved. We must give the reserved stock back before cancelling the
    # order, otherwise it would be stuck "reserved" forever with no
    # corresponding paid order.
    saga.state = SagaStateEnum.COMPENSATING
    saga.failure_reason = event.reason
    _emit(session, ReleaseInventory(order_id=event.order_id, sku=saga.sku, qty=saga.qty))
    return TransitionOutcome(applied=True)


async def handle_inventory_released(session: AsyncSession, payload: dict) -> TransitionOutcome:
    event = InventoryReleased.model_validate(payload)
    saga = await _get_saga(session, event.order_id)

    if saga is None:
        return TransitionOutcome(applied=False, reason="unknown_saga")
    if saga.state != SagaStateEnum.COMPENSATING:
        return TransitionOutcome(applied=False, reason=f"unexpected_state:{saga.state}")

    saga.state = SagaStateEnum.CANCELLED
    _emit(session, CancelOrder(order_id=event.order_id, reason=f"payment_failed:{saga.failure_reason}"))
    return TransitionOutcome(applied=True)


# Maps an incoming event's routing key to the handler that processes it.
HANDLERS = {
    OrderCreated.__name__: handle_order_created,
    InventoryReserved.__name__: handle_inventory_reserved,
    InventoryReservationFailed.__name__: handle_inventory_reservation_failed,
    PaymentSucceeded.__name__: handle_payment_succeeded,
    PaymentFailed.__name__: handle_payment_failed,
    InventoryReleased.__name__: handle_inventory_released,
}
