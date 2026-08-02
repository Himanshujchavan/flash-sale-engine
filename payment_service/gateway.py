"""
A real payment integration (Stripe, Braintree, etc.) is out of scope for a
portfolio project, but the SAGA'S COMPENSATION PATH is a core thing this
project is meant to demonstrate -- so the mock gateway deliberately fails
some fraction of the time (configurable), rather than always succeeding.
Without this, you could never actually see/demo the "release inventory
because payment failed" flow.
"""
from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass


@dataclass
class GatewayResult:
    success: bool
    gateway_ref: str
    failure_reason: str | None = None


# Tunable knobs for demoing both the happy path and the compensation path.
FAILURE_RATE = 0.25          # 25% of charges "decline" -- change to 0.0 to force all-success
MIN_LATENCY_SEC = 0.05
MAX_LATENCY_SEC = 0.3

_DECLINE_REASONS = [
    "insufficient_funds",
    "card_declined",
    "gateway_timeout",
    "fraud_check_failed",
]


async def charge(amount_cents: int, user_id: str) -> GatewayResult:
    """Simulates calling out to a payment processor."""
    await asyncio.sleep(random.uniform(MIN_LATENCY_SEC, MAX_LATENCY_SEC))

    if random.random() < FAILURE_RATE:
        return GatewayResult(
            success=False,
            gateway_ref=str(uuid.uuid4()),
            failure_reason=random.choice(_DECLINE_REASONS),
        )

    return GatewayResult(success=True, gateway_ref=str(uuid.uuid4()))


async def refund(payment_id: str, amount_cents: int) -> GatewayResult:
    """Simulates issuing a refund for a previously successful charge. Refunds
    essentially always succeed in real gateways (it's your own money going
    back out), so no failure injection here."""
    await asyncio.sleep(random.uniform(MIN_LATENCY_SEC, MAX_LATENCY_SEC))
    return GatewayResult(success=True, gateway_ref=str(uuid.uuid4()))
