"""
Order Service -- run with:
    uvicorn order_service.main:app --reload --port 8001

Responsibilities:
  1. POST /checkout -- create an order (status=PENDING) and an OrderCreated
     outbox row, atomically, in one local transaction. Returns immediately
     (202) -- the client does NOT wait for reservation/payment to finish.
  2. Background outbox dispatcher task -- polls the outbox table and
     publishes unpublished rows to RabbitMQ (see outbox.py).
  3. RabbitMQ consumer -- listens for `ConfirmOrder` / `CancelOrder`
     commands from the Saga Coordinator and updates order status.

Run `alembic upgrade head` (or the create_all bootstrap below, for a quick
demo) against order_db before starting this.
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from order_service.models import Base, Order, OutboxMessage
from order_service.outbox import outbox_dispatcher_loop
from shared.db import make_engine_and_session
from shared.events import CancelOrder, ConfirmOrder, OrderCancelled, OrderConfirmed, OrderCreated
from shared.logging_config import configure_logging
from shared.rabbitmq import RabbitMQClient

log = configure_logging("order_service")

engine, SessionLocal = make_engine_and_session("order_db")
rabbit_consumer = RabbitMQClient()  # separate connection dedicated to consuming commands

app = FastAPI(title="Order Service")

_background_tasks: list[asyncio.Task] = []


class CheckoutRequest(BaseModel):
    user_id: str
    sku: str
    qty: int = Field(gt=0)
    amount_cents: int = Field(gt=0)


class CheckoutResponse(BaseModel):
    order_id: str
    status: str


@app.on_event("startup")
async def startup() -> None:
    # Quick-start table creation for local dev. Swap for `alembic upgrade head`
    # once you add real migrations in Phase 8 polish.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await rabbit_consumer.connect()
    _background_tasks.append(asyncio.create_task(outbox_dispatcher_loop(SessionLocal, log)))
    _background_tasks.append(asyncio.create_task(_consume_saga_commands()))
    log.info("order_service_started")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in _background_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await rabbit_consumer.close()
    await engine.dispose()


@app.post("/checkout", response_model=CheckoutResponse, status_code=202)
async def checkout(req: CheckoutRequest) -> CheckoutResponse:
    async with SessionLocal() as session:
        order = Order(
            user_id=req.user_id,
            sku=req.sku,
            qty=req.qty,
            amount_cents=req.amount_cents,
            status="PENDING",
        )
        session.add(order)
        await session.flush()  # populate order.id without committing yet

        event = OrderCreated(
            order_id=order.id,
            sku=order.sku,
            qty=order.qty,
            user_id=order.user_id,
            amount_cents=order.amount_cents,
        )
        session.add(OutboxMessage(
            routing_key=event.routing_key,
            payload=event.model_dump(mode="json"),
        ))

        await session.commit()  # order row + outbox row committed atomically

    log.info("checkout_accepted", order_id=order.id, sku=order.sku, qty=order.qty)
    return CheckoutResponse(order_id=order.id, status=order.status)


@app.get("/orders/{order_id}", response_model=CheckoutResponse)
async def get_order(order_id: str) -> CheckoutResponse:
    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            raise HTTPException(status_code=404, detail="order not found")
        return CheckoutResponse(order_id=order.id, status=order.status)


async def _consume_saga_commands() -> None:
    """Listens for the Saga Coordinator's final commands, updates order
    status, and -- BUG FIX -- emits the corresponding OrderConfirmed /
    OrderCancelled EVENT via this service's own outbox. Previously this
    handler only updated `order.status` locally and never published
    anything further, so any other service wanting to react to an order's
    final outcome (e.g. Notification Service sending a confirmation or
    apology message) had nothing to subscribe to. The saga coordinator's
    ConfirmOrder/CancelOrder are COMMANDS addressed specifically to this
    service; they are not broadcast events other services should consume
    directly, so Order Service re-publishing its own EVENT after applying
    the command is exactly the right place for this to happen -- it's the
    one service that actually owns the "order" concept."""

    async def handle(routing_key: str, payload: dict) -> None:
        async with SessionLocal() as session:
            result = await session.execute(select(Order).where(Order.id == payload["order_id"]))
            order = result.scalar_one_or_none()
            if order is None:
                log.warning("order_not_found_for_command", routing_key=routing_key, payload=payload)
                return

            if routing_key == ConfirmOrder.__name__:
                order.status = "CONFIRMED"
                event = OrderConfirmed(order_id=order.id)
                log.info("order_confirmed", order_id=order.id)
            elif routing_key == CancelOrder.__name__:
                order.status = "CANCELLED"
                reason = payload.get("reason", "unknown")
                event = OrderCancelled(order_id=order.id, reason=reason)
                log.info("order_cancelled", order_id=order.id, reason=reason)
            else:
                return

            session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
            await session.commit()

    await rabbit_consumer.consume(
        queue_name="order_service.saga_commands",
        routing_keys=[ConfirmOrder.__name__, CancelOrder.__name__],
        handler=handle,
    )
