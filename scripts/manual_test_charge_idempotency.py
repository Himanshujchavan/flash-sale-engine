"""
Manual Phase 4 test -- run AFTER payment_service is running
(`uvicorn payment_service.main:app --port 8003`).

Publishes the SAME ChargePayment command twice (same order_id) to simulate
a redelivered message (e.g. RabbitMQ at-least-once redelivery after a
crash), and confirms the SECOND attempt returns the identical cached result
instead of charging again -- proving the idempotency guard works.

Usage:
    python scripts/manual_test_charge_idempotency.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.events import ChargePayment, PaymentFailed, PaymentSucceeded, new_id
from shared.rabbitmq import RabbitMQClient


async def send_charge_and_wait(rabbit: RabbitMQClient, order_id: str, amount_cents: int) -> dict:
    result_holder: dict = {}
    done = asyncio.Event()

    async def on_event(routing_key: str, payload: dict) -> None:
        if payload.get("order_id") != order_id:
            return
        result_holder["routing_key"] = routing_key
        result_holder["payload"] = payload
        done.set()

    await rabbit.consume(
        queue_name=f"manual_test_charge.{order_id}.{new_id()}",
        routing_keys=[PaymentSucceeded.__name__, PaymentFailed.__name__],
        handler=on_event,
    )

    cmd = ChargePayment(order_id=order_id, user_id="test-user", amount_cents=amount_cents)
    await rabbit.publish(cmd.routing_key, cmd.model_dump(mode="json"))

    await asyncio.wait_for(done.wait(), timeout=10.0)
    return result_holder


async def main() -> None:
    order_id = new_id()
    amount_cents = 4999

    rabbit = RabbitMQClient()
    await rabbit.connect()

    print(f"order_id={order_id}\n")

    print("--- First ChargePayment (should hit the real mock gateway) ---")
    first = await send_charge_and_wait(rabbit, order_id, amount_cents)
    print(f"Result: {first['routing_key']}")
    print(first["payload"])

    print("\n--- Second ChargePayment, SAME order_id (simulates redelivery) ---")
    second = await send_charge_and_wait(rabbit, order_id, amount_cents)
    print(f"Result: {second['routing_key']}")
    print(second["payload"])

    print("\n--- Verdict ---")
    same_outcome = first["routing_key"] == second["routing_key"]
    if first["routing_key"] == PaymentSucceeded.__name__:
        same_payment_id = first["payload"]["payment_id"] == second["payload"]["payment_id"]
        print(f"Same outcome: {same_outcome}, same payment_id (no double charge): {same_payment_id}")
    else:
        print(f"Same outcome (both declined, no second gateway call): {same_outcome}")

    await rabbit.close()


if __name__ == "__main__":
    asyncio.run(main())
