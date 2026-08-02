"""
Shared Redis helpers used by multiple services:

  - get_redis()            -> singleton async client
  - DistributedLock         -> simple single-node Redlock-style lock (SET NX PX + Lua unlock)
  - TokenBucketLimiter       -> Lua-scripted token bucket for rate limiting
  - IdempotencyStore         -> "have we already processed this key?" check with TTL

Note on Redlock: a *true* Redlock is quorum-based across multiple independent
Redis nodes. For this project we run a single Redis instance (fine for a
portfolio/demo project), so this is a simplified single-node lock using the
standard SET-NX-with-expiry + unique-token-and-Lua-unlock pattern, which is
the same primitive Redlock is built from. It's called out here so it's clear
this is a deliberate simplification, not a misunderstanding of Redlock.
"""
from __future__ import annotations

import uuid
from functools import lru_cache

import redis.asyncio as redis

from shared.settings import get_settings

_UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_TOKEN_BUCKET_SCRIPT = """
-- KEYS[1] = bucket key
-- ARGV[1] = capacity, ARGV[2] = refill_per_sec, ARGV[3] = now (float seconds), ARGV[4] = requested tokens
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    ts = now
end

local delta = math.max(0, now - ts)
tokens = math.min(capacity, tokens + delta * refill_per_sec)

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call("HMSET", KEYS[1], "tokens", tokens, "ts", now)
redis.call("EXPIRE", KEYS[1], 60)

return {allowed, tokens}
"""


@lru_cache
def get_redis() -> redis.Redis:
    settings = get_settings()
    return redis.from_url(settings.redis_url, decode_responses=True)


class DistributedLock:
    """
    Usage:
        lock = DistributedLock(f"lock:sku:{sku}")
        async with lock:
            ... critical section ...
    Raises LockAcquisitionError if the lock can't be acquired within `timeout_sec`.
    """

    def __init__(self, key: str, ttl_ms: int = 5000, timeout_sec: float = 3.0, retry_delay_sec: float = 0.05):
        self.key = key
        self.ttl_ms = ttl_ms
        self.timeout_sec = timeout_sec
        self.retry_delay_sec = retry_delay_sec
        self.token = str(uuid.uuid4())
        self._redis = get_redis()

    async def acquire(self) -> bool:
        import asyncio
        import time

        deadline = time.monotonic() + self.timeout_sec
        while time.monotonic() < deadline:
            got = await self._redis.set(self.key, self.token, nx=True, px=self.ttl_ms)
            if got:
                return True
            await asyncio.sleep(self.retry_delay_sec)
        return False

    async def release(self) -> None:
        await self._redis.eval(_UNLOCK_SCRIPT, 1, self.key, self.token)

    async def __aenter__(self) -> "DistributedLock":
        acquired = await self.acquire()
        if not acquired:
            raise LockAcquisitionError(f"Could not acquire lock for key={self.key}")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()


class LockAcquisitionError(Exception):
    pass


class TokenBucketLimiter:
    """
    Per-key (e.g. per-SKU, or per-client-ip) token bucket rate limiter.
    Atomic via a Lua script so concurrent requests can't race each other's read-modify-write.
    """

    def __init__(self, capacity: int | None = None, refill_per_sec: float | None = None):
        settings = get_settings()
        self.capacity = capacity or settings.rate_limit_capacity
        self.refill_per_sec = refill_per_sec or settings.rate_limit_refill_per_sec
        self._redis = get_redis()

    async def allow(self, key: str, tokens: int = 1) -> bool:
        import time

        bucket_key = f"ratelimit:{key}"
        result = await self._redis.eval(
            _TOKEN_BUCKET_SCRIPT, 1, bucket_key,
            self.capacity, self.refill_per_sec, time.time(), tokens,
        )
        allowed = int(result[0]) == 1
        return allowed


class IdempotencyStore:
    """
    Used by the Payment Service to guard against double-charging when the
    same ChargePayment command is redelivered (RabbitMQ at-least-once
    delivery, or the outbox dispatcher republishing after a crash).
    """

    def __init__(self, ttl_sec: int = 86400):
        self.ttl_sec = ttl_sec
        self._redis = get_redis()

    def _key(self, idempotency_key: str) -> str:
        return f"idempotency:{idempotency_key}"

    async def get_cached_result(self, idempotency_key: str) -> str | None:
        return await self._redis.get(self._key(idempotency_key))

    async def store_result(self, idempotency_key: str, result_json: str) -> None:
        await self._redis.set(self._key(idempotency_key), result_json, ex=self.ttl_sec)
