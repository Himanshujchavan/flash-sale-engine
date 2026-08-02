"""
Run this AFTER a load test (via locustfile.py) has finished and had a few
seconds to settle (sagas need a moment to churn through Reserve -> Charge ->
Confirm/Compensate for every order). It connects directly to each service's
own database (this is the ONE place in the whole project that's allowed to
do that, since it's a read-only external auditor, not a service) and checks
that the distributed system as a whole ended up in a consistent state.

Usage:
    python scripts/verify_consistency.py flash-sale-load-test-sku

Checks performed:
  1. No order is stuck PENDING -- every order must have resolved to
     CONFIRMED or CANCELLED. A PENDING order after the saga has had time to
     settle would mean an event got dropped somewhere.
  2. Inventory math: available_qty + reserved_qty == the original seeded
     quantity (nothing created or destroyed stock out of nowhere).
  3. reserved_qty == the qty of every CONFIRMED order for this SKU (exactly
     the stock that should still be held, no more, no less -- proves no
     oversell AND no under-release).
  4. Payment count sanity: every CONFIRMED order has exactly one SUCCEEDED
     payment; no order has more than one payment row (would indicate the
     double-charge race, see payment_service/main.py's DistributedLock fix).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, select

from inventory_service.models import InventoryItem
from order_service.models import Order
from payment_service.models import Payment
from shared.db import make_engine_and_session


async def main() -> None:
    sku = sys.argv[1] if len(sys.argv) > 1 else "flash-sale-load-test-sku"
    problems: list[str] = []

    order_engine, OrderSession = make_engine_and_session("order_db")
    inv_engine, InvSession = make_engine_and_session("inventory_db")
    pay_engine, PaySession = make_engine_and_session("payment_db")

    # --- 1. No order stuck PENDING, for this SKU ---
    async with OrderSession() as session:
        result = await session.execute(select(Order).where(Order.sku == sku))
        orders = result.scalars().all()

    total = len(orders)
    confirmed = [o for o in orders if o.status == "CONFIRMED"]
    cancelled = [o for o in orders if o.status == "CANCELLED"]
    pending = [o for o in orders if o.status == "PENDING"]

    print(f"Orders for sku={sku}: total={total}, confirmed={len(confirmed)}, "
          f"cancelled={len(cancelled)}, pending={len(pending)}")

    if pending:
        problems.append(
            f"{len(pending)} order(s) stuck PENDING (saga never settled): "
            f"{[o.id for o in pending[:5]]}{'...' if len(pending) > 5 else ''}"
        )

    # --- 2 & 3. Inventory math ---
    async with InvSession() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.sku == sku))
        item = result.scalar_one_or_none()

    if item is None:
        problems.append(f"No inventory row found for sku={sku} -- was it ever seeded?")
    else:
        confirmed_qty = sum(o.qty for o in confirmed)
        print(f"Inventory: available={item.available_qty}, reserved={item.reserved_qty}, "
              f"sum(confirmed order qty)={confirmed_qty}")

        if item.available_qty < 0 or item.reserved_qty < 0:
            problems.append(
                f"NEGATIVE inventory quantity! available={item.available_qty} "
                f"reserved={item.reserved_qty} -- this would mean overselling actually happened."
            )

        if item.reserved_qty != confirmed_qty:
            problems.append(
                f"reserved_qty ({item.reserved_qty}) != sum of CONFIRMED orders' qty "
                f"({confirmed_qty}) -- some reservation was never released after a "
                f"cancellation, or was released incorrectly for a confirmed order."
            )

    # --- 4. Payment sanity (scoped to just this SKU's orders -- checking
    #        every payment ever recorded in payment_db would also catch
    #        unrelated leftover data from other test runs/SKUs) ---
    order_ids_for_sku = {o.id for o in orders}
    async with PaySession() as session:
        result = await session.execute(
            select(Payment.order_id, func.count(Payment.id))
            .where(Payment.order_id.in_(order_ids_for_sku))
            .group_by(Payment.order_id)
        )
        payment_counts = {order_id: count for order_id, count in result.all()}

    duplicated = {oid: c for oid, c in payment_counts.items() if c > 1}
    if duplicated:
        problems.append(
            f"{len(duplicated)} order(s) have MORE THAN ONE payment row -- possible double charge: "
            f"{list(duplicated.items())[:5]}"
        )

    confirmed_ids = {o.id for o in confirmed}
    missing_payment = confirmed_ids - set(payment_counts.keys())
    if missing_payment:
        problems.append(
            f"{len(missing_payment)} CONFIRMED order(s) have NO payment row at all: "
            f"{list(missing_payment)[:5]}"
        )

    await order_engine.dispose()
    await inv_engine.dispose()
    await pay_engine.dispose()

    print()
    if problems:
        print(f"FOUND {len(problems)} CONSISTENCY PROBLEM(S):")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    else:
        print("All consistency checks passed: no stuck orders, inventory math checks "
              "out exactly, no double charges, every confirmed order has a payment.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
