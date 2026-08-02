"""
Notification Service -- run with:
    uvicorn notification_service.main:app --reload --port 8005

Responsibilities:
  1. RabbitMQ consumer -- OrderConfirmed -> "send" a confirmation
     notification. OrderCancelled -> "send" a cancellation/apology
     notification (with the reason, so the customer knows why -- e.g.
     "your card was declined" vs "that item just sold out").
  2. Admin/demo endpoints -- list notifications, so you can see what a
     customer would have received without needing a real email/SMS
     provider hooked up.

This is a TERMINAL consumer: it's the last stop in the saga's lifecycle
and never publishes anything further, so there's no outbox table or
outbox dispatcher here (see notification_service/models.py's docstring).
Writing the Notification row directly and committing is enough, since
there's no second system (a message broker) that needs to be kept in sync
with the database write.
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI, HTTPException
from sqlalchemy import select

from notification_service.models import Base, Notification
from shared.db import make_engine_and_session
from shared.events import OrderCancelled, OrderConfirmed
from shared.logging_config import configure_logging
from shared.rabbitmq import RabbitMQClient

log = configure_logging("notification_service")

engine, SessionLocal = make_engine_and_session("notification_db")
rabbit_consumer = RabbitMQClient()

app = FastAPI(title="Notification Service")

_background_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await rabbit_consumer.connect()
    _background_tasks.append(asyncio.create_task(_consume_order_outcomes()))
    log.info("notification_service_started")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in _background_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await rabbit_consumer.close()
    await engine.dispose()


@app.get("/notifications/{order_id}")
async def get_notifications_for_order(order_id: str) -> list[dict]:
    async with SessionLocal() as session:
        result = await session.execute(
            select(Notification).where(Notification.order_id == order_id).order_by(Notification.created_at)
        )
        notifications = result.scalars().all()
        if not notifications:
            raise HTTPException(status_code=404, detail="no notifications found for this order_id")
        return [_serialize(n) for n in notifications]


@app.get("/notifications")
async def list_recent_notifications(limit: int = 20) -> list[dict]:
    async with SessionLocal() as session:
        result = await session.execute(
            select(Notification).order_by(Notification.created_at.desc()).limit(limit)
        )
        return [_serialize(n) for n in result.scalars().all()]


def _serialize(n: Notification) -> dict:
    return {
        "id": n.id,
        "order_id": n.order_id,
        "type": n.notification_type,
        "message": n.message,
        "created_at": n.created_at.isoformat(),
    }


async def _consume_order_outcomes() -> None:
    async def handle(routing_key: str, payload: dict) -> None:
        async with SessionLocal() as session:
            if routing_key == OrderConfirmed.__name__:
                event = OrderConfirmed.model_validate(payload)
                message = f"Your order {event.order_id} is confirmed! It's on its way."
                notification_type = "ORDER_CONFIRMED"

            elif routing_key == OrderCancelled.__name__:
                event = OrderCancelled.model_validate(payload)
                message = _friendly_cancellation_message(event.order_id, event.reason)
                notification_type = "ORDER_CANCELLED"

            else:
                return

            session.add(Notification(order_id=event.order_id, notification_type=notification_type, message=message))
            await session.commit()

            log.info(
                "notification_sent",
                order_id=event.order_id,
                type=notification_type,
                message=message,
            )

    await rabbit_consumer.consume(
        queue_name="notification_service.order_outcomes",
        routing_keys=[OrderConfirmed.__name__, OrderCancelled.__name__],
        handler=handle,
    )


def _friendly_cancellation_message(order_id: str, reason: str) -> str:
    """Translate the internal machine-readable reason string (e.g.
    "payment_failed:card_declined" or "inventory_unavailable:unknown_sku")
    into something a customer could actually read, rather than leaking
    internal saga/service jargon straight into a customer-facing message."""
    if reason.startswith("payment_failed"):
        return f"Sorry, order {order_id} couldn't go through -- your payment was declined. You have not been charged."
    if reason.startswith("inventory_unavailable"):
        return f"Sorry, order {order_id} couldn't be completed -- that item just sold out."
    return f"Sorry, order {order_id} was cancelled ({reason})."
