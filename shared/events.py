"""
Shared event & command schemas.

Naming convention:
  - Events are things that HAVE HAPPENED (past tense): OrderCreated, InventoryReserved
  - Commands are requests for something TO HAPPEN (imperative): ReserveInventory, ChargePayment

Both travel over the same RabbitMQ topic exchange (settings.events_exchange),
routed by `routing_key` (== the class name, e.g. "OrderCreated").

Every message carries `order_id` so any consumer/saga can correlate it,
plus a `message_id` (for idempotency / dedup) and `occurred_at`.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class BaseMessage(BaseModel):
    message_id: str = Field(default_factory=new_id)
    order_id: str
    occurred_at: datetime = Field(default_factory=_now)

    @property
    def routing_key(self) -> str:
        return self.__class__.__name__


# ---------------------------------------------------------------- Events --

class OrderCreated(BaseMessage):
    sku: str
    qty: int
    user_id: str
    amount_cents: int


class InventoryReserved(BaseMessage):
    sku: str
    qty: int


class InventoryReservationFailed(BaseMessage):
    sku: str
    qty: int
    reason: str


class InventoryReleased(BaseMessage):
    sku: str
    qty: int


class PaymentSucceeded(BaseMessage):
    payment_id: str
    amount_cents: int


class PaymentFailed(BaseMessage):
    reason: str


class PaymentRefunded(BaseMessage):
    payment_id: str
    amount_cents: int


class OrderConfirmed(BaseMessage):
    pass


class OrderCancelled(BaseMessage):
    reason: str


# -------------------------------------------------------------- Commands --

class ReserveInventory(BaseMessage):
    sku: str
    qty: int


class ReleaseInventory(BaseMessage):
    sku: str
    qty: int


class ChargePayment(BaseMessage):
    user_id: str
    amount_cents: int


class RefundPayment(BaseMessage):
    payment_id: str
    amount_cents: int


class ConfirmOrder(BaseMessage):
    pass


class CancelOrder(BaseMessage):
    reason: str


# Registry used by consumers to deserialize an incoming message by routing key
MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    cls.__name__: cls
    for cls in [
        OrderCreated, InventoryReserved, InventoryReservationFailed, InventoryReleased,
        PaymentSucceeded, PaymentFailed, PaymentRefunded, OrderConfirmed, OrderCancelled,
        ReserveInventory, ReleaseInventory, ChargePayment, RefundPayment, ConfirmOrder, CancelOrder,
    ]
}


class SagaState(str):
    """String enum-like constants for saga_coordinator's state machine (kept here so
    other services can reference states in logs/tests without importing the coordinator)."""
    CREATED = "created"
    RESERVING = "reserving"
    RESERVED = "reserved"
    CHARGING = "charging"
    CHARGED = "charged"
    CONFIRMED = "confirmed"
    RESERVATION_FAILED = "reservation_failed"
    PAYMENT_FAILED = "payment_failed"
    COMPENSATING = "compensating"
    CANCELLED = "cancelled"
