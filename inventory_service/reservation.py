"""
This is the critical-path code the whole "thundering herd" story is about.
Order of operations for a reservation attempt matters:

  1. Token-bucket rate limit check (cheap, in-memory-ish via Redis, no lock
     needed) -- rejects excess traffic BEFORE we ever try to take a lock or
     touch Postgres. This is what actually protects the DB under a flash-sale
     spike: most of the herd gets turned away here, fast.
  2. Redis distributed lock, keyed per-SKU -- serializes the read-check-write
     for a single SKU so concurrent reservation attempts can't both read
     "5 available" and both decrement, causing an oversell.
  3. INSIDE that same lock: an idempotency check keyed on order_id -- see
     "BUG FIX" below.
  4. Inside the lock: a single local Postgres transaction that checks
     available_qty and, if sufficient, atomically moves qty from
     available_qty to reserved_qty.

Releasing (compensation path) does the reverse move and does NOT need the
rate limiter (it's not client-facing demand, it's an internal correction),
but it DOES still need the lock, since a release could race with a fresh
reservation attempt on the same SKU.

BUG FIX -- idempotency: RabbitMQ's at-least-once delivery means a
ReserveInventory (or ReleaseInventory) command CAN be redelivered for a
command that already succeeded (e.g. a network blip between this service
acking the message and the broker registering the ack, or the outbox
dispatcher on the SENDING side republishing after a crash). Without a
guard, a redelivered ReserveInventory would decrement available_qty and
increment reserved_qty a SECOND time for the same order -- but the Saga
Coordinator's own state-machine guard means it only reacts to the FIRST
InventoryReserved event for that order (a redelivered/duplicate one is a
no-op there, since the saga has already moved past RESERVING). The result:
inventory silently ends up holding one extra "phantom" reserved unit
forever, with nothing else in the system aware of or accounting for it --
exactly the kind of quiet drift a consistency audit (scripts/verify_consistency.py)
is designed to catch. The fix mirrors Payment Service's idempotency guard,
but doesn't need its own separate lock: the per-SKU DistributedLock already
serializes every reservation/release attempt for a SKU, so checking and
storing the order-level idempotency key INSIDE that same critical section
is already race-free.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from inventory_service.models import InventoryItem
from shared.redis_client import DistributedLock, IdempotencyStore, LockAcquisitionError, TokenBucketLimiter


@dataclass
class ReservationResult:
    success: bool
    reason: str | None = None


async def try_reserve(
    session: AsyncSession,
    order_id: str,
    sku: str,
    qty: int,
    rate_limiter: TokenBucketLimiter,
    idempotency: IdempotencyStore,
) -> ReservationResult:
    idem_key = f"reserve:{order_id}"

    lock = DistributedLock(key=f"lock:sku:{sku}", ttl_ms=5000, timeout_sec=3.0)
    try:
        async with lock:
            cached = await idempotency.get_cached_result(idem_key)
            if cached is not None:
                data = json.loads(cached)
                return ReservationResult(success=data["success"], reason=data.get("reason"))

            # Rate limiting happens INSIDE the lock, after the idempotency
            # check -- a redelivered command for an order already resolved
            # should never spend a token or contend the lock's DB work; it
            # should short-circuit as cheaply as possible above.
            allowed = await rate_limiter.allow(key=sku, tokens=1)
            if not allowed:
                outcome = ReservationResult(success=False, reason="rate_limited")
                # Deliberately NOT cached -- rate-limit rejections aren't
                # permanent outcomes the way a real reservation decision is;
                # the saga (or a human) may legitimately want a retry of the
                # same order_id to get a fresh shot at the bucket rather than
                # being stuck with a stale rejection forever.
                return outcome

            result = await session.execute(
                select(InventoryItem).where(InventoryItem.sku == sku).with_for_update()
            )
            item = result.scalar_one_or_none()

            if item is None:
                outcome = ReservationResult(success=False, reason="unknown_sku")
            elif item.available_qty < qty:
                outcome = ReservationResult(success=False, reason="insufficient_stock")
            else:
                item.available_qty -= qty
                item.reserved_qty += qty
                await session.flush()  # caller commits together with the outbox row
                outcome = ReservationResult(success=True)

            await idempotency.store_result(
                idem_key, json.dumps({"success": outcome.success, "reason": outcome.reason})
            )
            return outcome
    except LockAcquisitionError:
        return ReservationResult(success=False, reason="lock_timeout")


async def release_reservation(session: AsyncSession, order_id: str, sku: str, qty: int,
                               idempotency: IdempotencyStore) -> None:
    idem_key = f"release:{order_id}"

    lock = DistributedLock(key=f"lock:sku:{sku}", ttl_ms=5000, timeout_sec=3.0)
    async with lock:
        cached = await idempotency.get_cached_result(idem_key)
        if cached is not None:
            return  # already released for this order -- redelivered command, no-op

        result = await session.execute(
            select(InventoryItem).where(InventoryItem.sku == sku).with_for_update()
        )
        item = result.scalar_one_or_none()
        if item is None:
            return  # nothing to release against; log at call site

        item.reserved_qty = max(0, item.reserved_qty - qty)
        item.available_qty += qty
        await session.flush()  # caller commits together with the outbox row

        await idempotency.store_result(idem_key, json.dumps({"released": True}))
