"""
Saga Coordinator -- run with:
    uvicorn saga_coordinator.main:app --reload --port 8004

Responsibilities:
  1. RabbitMQ consumer -- listens for every event in the lifecycle
     (OrderCreated, InventoryReserved, InventoryReservationFailed,
     PaymentSucceeded, PaymentFailed, InventoryReleased) and applies
     saga_coordinator/transitions.py's transition table, which both updates
     saga state and emits the next command as an outbox row.
  2. Admin/demo endpoint -- GET /sagas/{order_id} to inspect where an order
     currently sits in the workflow.
  3. Background outbox dispatcher (shared/outbox.py).

This is the ONLY service that listens to every event type -- every other
service only listens for the specific commands addressed to it. That
asymmetry is intentional: the saga coordinator is the one place the
overall workflow logic lives, so it's the one place that needs the full
picture.
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI, HTTPException
from sqlalchemy import select

from saga_coordinator.models import Base, OutboxMessage
from saga_coordinator.models import SagaState as SagaStateRow
from saga_coordinator.transitions import HANDLERS
from shared.db import make_engine_and_session
from shared.logging_config import configure_logging
from shared.outbox import run_outbox_dispatcher
from shared.rabbitmq import RabbitMQClient

log = configure_logging("saga_coordinator")

engine, SessionLocal = make_engine_and_session("saga_db")
rabbit_consumer = RabbitMQClient()

app = FastAPI(title="Saga Coordinator")

_background_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await rabbit_consumer.connect()
    _background_tasks.append(
        asyncio.create_task(run_outbox_dispatcher(SessionLocal, OutboxMessage, log, "saga_coordinator"))
    )
    _background_tasks.append(asyncio.create_task(_consume_lifecycle_events()))
    log.info("saga_coordinator_started")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in _background_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await rabbit_consumer.close()
    await engine.dispose()


@app.get("/sagas/{order_id}")
async def get_saga(order_id: str) -> dict:
    async with SessionLocal() as session:
        result = await session.execute(select(SagaStateRow).where(SagaStateRow.order_id == order_id))
        saga = result.scalar_one_or_none()
        if saga is None:
            raise HTTPException(status_code=404, detail="no saga found for this order_id")
        return {
            "order_id": saga.order_id,
            "state": saga.state,
            "sku": saga.sku,
            "qty": saga.qty,
            "amount_cents": saga.amount_cents,
            "payment_id": saga.payment_id,
            "failure_reason": saga.failure_reason,
        }


async def _consume_lifecycle_events() -> None:
    async def handle(routing_key: str, payload: dict) -> None:
        handler = HANDLERS.get(routing_key)
        if handler is None:
            log.warning("no_handler_for_event", routing_key=routing_key)
            return

        async with SessionLocal() as session:
            outcome = await handler(session, payload)
            if outcome.applied:
                await session.commit()
                log.info(
                    "saga_transition_applied",
                    routing_key=routing_key,
                    order_id=payload.get("order_id"),
                )
            else:
                # Not an error -- this is the guard-clause path that makes
                # the coordinator safe against duplicate/out-of-order
                # delivery. Nothing to commit since no changes were made.
                log.info(
                    "saga_transition_skipped",
                    routing_key=routing_key,
                    order_id=payload.get("order_id"),
                    reason=outcome.reason,
                )

    await rabbit_consumer.consume(
        queue_name="saga_coordinator.lifecycle_events",
        routing_keys=list(HANDLERS.keys()),
        handler=handle,
    )
