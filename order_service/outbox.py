"""
Thin re-export so existing imports (`from order_service.outbox import
outbox_dispatcher_loop`) keep working. The real implementation lives in
shared/outbox.py so Inventory/Payment services can reuse it without
duplicating the polling logic.
"""
from __future__ import annotations

from order_service.models import OutboxMessage
from shared.outbox import run_outbox_dispatcher


async def outbox_dispatcher_loop(session_factory, log) -> None:
    await run_outbox_dispatcher(session_factory, OutboxMessage, log, service_name="order_service")
