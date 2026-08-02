"""
Manual Phase 3 test -- run AFTER seeding stock via the HTTP endpoint and
AFTER inventory_service is running (`uvicorn inventory_service.main:app
--port 8002`).

This bypasses the (not-yet-built) Saga Coordinator and publishes a
ReserveInventory command directly, then listens for whatever event comes
back (InventoryReserved or InventoryReservationFailed), so you can verify
the distributed lock + rate limiter + reservation logic works end-to-end.

Usage (from project root, venv activated):
    python scripts/manual_test_reserve.py sneaker-42 1
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Allow running this script directly (`python scripts/manual_test_reserve.py`)
# from anywhere, by ensuring the project root (parent of this scripts/ dir)
# is on sys.path -- otherwise `from shared...` fails with ModuleNotFoundError
# since Python only auto-adds the script's OWN directory, not the project root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.events import (
    InventoryReservationFailed,
    InventoryReserved,
    ReserveInventory,
    new_id,
)
from shared.rabbitmq import RabbitMQClient


async def main() -> None:
    sku = sys.argv[1] if len(sys.argv) > 1 else "sneaker-42"
    qty = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    order_id = new_id()

    rabbit = RabbitMQClient()
    await rabbit.connect()

    result_holder: dict = {}
    done = asyncio.Event()

    async def on_event(routing_key: str, payload: dict) -> None:
        if payload.get("order_id") != order_id:
            return  # ignore events from other test runs / real traffic
        result_holder["routing_key"] = routing_key
        result_holder["payload"] = payload
        done.set()

    await rabbit.consume(
        queue_name=f"manual_test.{order_id}",
        routing_keys=[InventoryReserved.__name__, InventoryReservationFailed.__name__],
        handler=on_event,
    )

    cmd = ReserveInventory(order_id=order_id, sku=sku, qty=qty)
    print(f"Publishing ReserveInventory: order_id={order_id} sku={sku} qty={qty}")
    await rabbit.publish(cmd.routing_key, cmd.model_dump(mode="json"))

    try:
        await asyncio.wait_for(done.wait(), timeout=10.0)
        print(f"Result: {result_holder['routing_key']}")
        print(result_holder["payload"])
    except asyncio.TimeoutError:
        print("Timed out waiting for a response -- is inventory_service running?")
    finally:
        await rabbit.close()


if __name__ == "__main__":
    asyncio.run(main())
