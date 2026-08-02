"""
Payment Service -- run with:
    uvicorn payment_service.main:app --reload --port 8003

Responsibilities:
  1. RabbitMQ consumer -- ChargePayment -> (idempotency check) -> mock
     gateway.charge() -> publishes PaymentSucceeded or PaymentFailed.
                          RefundPayment -> mock gateway.refund() ->
     publishes PaymentRefunded. (Compensation path, triggered when a later
     saga step fails after payment already succeeded -- not needed in THIS
     saga's happy/failure paths since payment is the last chargeable step,
     but included for completeness / if you extend the saga later, e.g.
     inventory fulfillment failing after payment succeeded.)
  2. Admin/demo endpoint -- GET /payments/{order_id} to inspect a payment.
  3. Background outbox dispatcher (shared/outbox.py).

IDEMPOTENCY -- why it matters here specifically: RabbitMQ's consumer.ack()
model means a ChargePayment command WILL occasionally be redelivered (e.g.
this service crashes after charging the card but before acking the
message). Without the idempotency guard below, that redelivery would charge
the customer's card twice. The guard keys on `order_id` (one order = one
charge, ever) and caches the outcome in Redis so a redelivered command
short-circuits straight to "here's what already happened" instead of
calling the gateway again.
"""
from __future__ import annotations

import asyncio
import contextlib
import json

from fastapi import FastAPI, HTTPException
from sqlalchemy import select

from payment_service import gateway
from payment_service.models import Base, OutboxMessage, Payment
from shared.db import make_engine_and_session
from shared.events import ChargePayment, PaymentFailed, PaymentRefunded, PaymentSucceeded, RefundPayment
from shared.logging_config import configure_logging
from shared.outbox import run_outbox_dispatcher
from shared.rabbitmq import RabbitMQClient
from shared.redis_client import DistributedLock, IdempotencyStore

log = configure_logging("payment_service")

engine, SessionLocal = make_engine_and_session("payment_db")
rabbit_consumer = RabbitMQClient()
idempotency = IdempotencyStore()

app = FastAPI(title="Payment Service")

_background_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await rabbit_consumer.connect()
    _background_tasks.append(
        asyncio.create_task(run_outbox_dispatcher(SessionLocal, OutboxMessage, log, "payment_service"))
    )
    _background_tasks.append(asyncio.create_task(_consume_saga_commands()))
    log.info("payment_service_started")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in _background_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await rabbit_consumer.close()
    await engine.dispose()


@app.get("/payments/{order_id}")
async def get_payment(order_id: str) -> dict:
    async with SessionLocal() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        payment = result.scalars().first()
        if payment is None:
            raise HTTPException(status_code=404, detail="no payment found for this order_id")
        return {
            "payment_id": payment.id,
            "order_id": payment.order_id,
            "amount_cents": payment.amount_cents,
            "status": payment.status,
            "failure_reason": payment.failure_reason,
        }


async def _consume_saga_commands() -> None:
    async def handle(routing_key: str, payload: dict) -> None:
        async with SessionLocal() as session:
            if routing_key == ChargePayment.__name__:
                await _handle_charge(session, ChargePayment.model_validate(payload))
            elif routing_key == RefundPayment.__name__:
                await _handle_refund(session, RefundPayment.model_validate(payload))

    await rabbit_consumer.consume(
        queue_name="payment_service.saga_commands",
        routing_keys=[ChargePayment.__name__, RefundPayment.__name__],
        handler=handle,
    )


async def _handle_charge(session, cmd: ChargePayment) -> None:
    idem_key = f"charge:{cmd.order_id}"

    # BUG FIX: the idempotency check ("is this order already charged?") and
    # the act (charge + store result) are two separate steps with a real gap
    # between them (gateway call + DB commit). Without a lock, two concurrent
    # deliveries of the same ChargePayment (e.g. a genuine RabbitMQ
    # redelivery landing while the first delivery is still mid-flight) can
    # BOTH see "not cached yet" and BOTH charge the card. The lock below
    # serializes processing per order_id so the second delivery always waits
    # for the first to finish (and cache its result) before it even checks.
    lock = DistributedLock(key=f"lock:payment:{cmd.order_id}", ttl_ms=10_000, timeout_sec=8.0)
    async with lock:
        cached = await idempotency.get_cached_result(idem_key)
        if cached is not None:
            # This ChargePayment command was already processed (redelivery from
            # RabbitMQ's at-least-once guarantee, or the outbox dispatcher
            # republishing after a crash). Re-emit the SAME outcome instead of
            # calling the gateway again -- this is what prevents double-charging.
            cached_data = json.loads(cached)
            log.info("charge_idempotent_hit", order_id=cmd.order_id, cached_status=cached_data["status"])
            event = _event_from_cached_charge(cmd.order_id, cached_data)
            session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
            await session.commit()
            return

        result = await gateway.charge(cmd.amount_cents, cmd.user_id)

        payment = Payment(
            order_id=cmd.order_id,
            user_id=cmd.user_id,
            amount_cents=cmd.amount_cents,
            status="SUCCEEDED" if result.success else "FAILED",
            gateway_ref=result.gateway_ref,
            failure_reason=result.failure_reason,
        )
        session.add(payment)
        await session.flush()  # populate payment.id

        cache_payload = {
            "status": payment.status,
            "payment_id": payment.id,
            "amount_cents": payment.amount_cents,
            "failure_reason": payment.failure_reason,
        }

        if result.success:
            event = PaymentSucceeded(order_id=cmd.order_id, payment_id=payment.id, amount_cents=cmd.amount_cents)
            log.info("payment_succeeded", order_id=cmd.order_id, payment_id=payment.id)
        else:
            event = PaymentFailed(order_id=cmd.order_id, reason=result.failure_reason or "unknown")
            log.warning("payment_failed", order_id=cmd.order_id, reason=result.failure_reason)

        session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
        await session.commit()

        # Store AFTER commit succeeds -- if the commit fails, we want the command
        # redelivered and retried for real, not short-circuited by a cached
        # result for a charge that never actually made it to the database.
        # Still happens INSIDE the lock so no other concurrent delivery can
        # slip past the cache check before this is written.
        await idempotency.store_result(idem_key, json.dumps(cache_payload))


def _event_from_cached_charge(order_id: str, cached: dict) -> PaymentSucceeded | PaymentFailed:
    if cached["status"] == "SUCCEEDED":
        return PaymentSucceeded(order_id=order_id, payment_id=cached["payment_id"], amount_cents=cached["amount_cents"])
    return PaymentFailed(order_id=order_id, reason=cached.get("failure_reason") or "unknown")


async def _handle_refund(session, cmd: RefundPayment) -> None:
    idem_key = f"refund:{cmd.payment_id}"

    # Same race as _handle_charge above, keyed on payment_id instead of
    # order_id since a refund is scoped to a specific payment.
    lock = DistributedLock(key=f"lock:refund:{cmd.payment_id}", ttl_ms=10_000, timeout_sec=8.0)
    async with lock:
        cached = await idempotency.get_cached_result(idem_key)
        if cached is not None:
            log.info("refund_idempotent_hit", payment_id=cmd.payment_id)
            event = PaymentRefunded(order_id=cmd.order_id, payment_id=cmd.payment_id, amount_cents=cmd.amount_cents)
            session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
            await session.commit()
            return

        result = await gateway.refund(cmd.payment_id, cmd.amount_cents)

        db_result = await session.execute(select(Payment).where(Payment.id == cmd.payment_id))
        payment = db_result.scalar_one_or_none()
        if payment is not None:
            payment.status = "REFUNDED"

        event = PaymentRefunded(order_id=cmd.order_id, payment_id=cmd.payment_id, amount_cents=cmd.amount_cents)
        session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
        await session.commit()

        await idempotency.store_result(idem_key, json.dumps({"gateway_ref": result.gateway_ref}))
        log.info("payment_refunded", order_id=cmd.order_id, payment_id=cmd.payment_id)
