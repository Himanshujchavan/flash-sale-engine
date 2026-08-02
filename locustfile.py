"""
Load test simulating a flash-sale traffic spike against /checkout.

Run with (from project root, venv activated, all 5 services + infra up):
    locust -f locustfile.py --host http://localhost:8001

Then open http://localhost:8089, and pick your user count / spawn rate.
For a real "flash sale spike" feel, use a HIGH spawn rate (e.g. 200 users,
spawn rate 50/sec) so load ramps up almost immediately rather than
gradually -- that's the actual failure mode this whole project exists to
survive (thundering herd on a single SKU the moment it drops).

WHAT THIS PROVES: the point isn't raw throughput numbers (this runs against
a sandboxed dev setup, not production hardware) -- it's that under
concurrent load, the system produces a CONSISTENT final state: every order
ends up CONFIRMED or CANCELLED (never stuck PENDING), inventory never goes
negative or gets oversold, and no order gets double-charged. Run
scripts/verify_consistency.py after a load test finishes churning through
the saga to check exactly that.
"""
from __future__ import annotations

import random

import requests
from locust import HttpUser, between, events, task

INVENTORY_SERVICE_URL = "http://localhost:8002"
LOAD_TEST_SKU = "phase7-idempotency-verified"
SEED_QTY = 500  # deliberately limited stock -- this is a flash sale, not infinite inventory


@events.test_start.add_listener
def seed_inventory(environment, **kwargs) -> None:
    """Locust's test_start fires once, before any simulated users start
    sending requests -- exactly where a one-time setup step like seeding
    stock belongs. Uses a plain `requests` call directly to Inventory
    Service since Locust's HttpUser is scoped to a single --host (Order
    Service), and this needs to hit a different port."""
    resp = requests.post(
        f"{INVENTORY_SERVICE_URL}/inventory/seed",
        json={"sku": LOAD_TEST_SKU, "qty": SEED_QTY},
        timeout=5,
    )
    resp.raise_for_status()
    print(f"[load test setup] Seeded {LOAD_TEST_SKU} with qty={SEED_QTY}: {resp.json()}")


class FlashSaleShopper(HttpUser):
    """Simulates a shopper hammering the checkout button during a flash
    sale. `wait_time` is intentionally tiny -- real flash-sale traffic
    doesn't politely space itself out."""

    wait_time = between(0.05, 0.3)

    @task
    def checkout(self) -> None:
        user_id = f"load-user-{random.randint(1, 1_000_000)}"
        self.client.post(
            "/checkout",
            json={
                "user_id": user_id,
                "sku": LOAD_TEST_SKU,
                "qty": 1,
                "amount_cents": random.choice([1999, 2999, 4999, 9999]),
            },
            name="/checkout",
        )
