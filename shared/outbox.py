"""
Generic transactional-outbox dispatcher, reused by every service (Order,
Inventory, Payment, ...). Each service defines its own OutboxMessage model
in its own database (see order_service/models.py for the canonical shape:
id, routing_key, payload (JSON), published (bool), created_at) and passes
that model class in here.

Guarantee: AT LEAST ONCE delivery -- see order_service/outbox.py's original
docstring for the full rationale. Every consumer of these events must be
idempotent as a result.
"""
from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from shared.rabbitmq import RabbitMQClient
from shared.settings import get_settings


async def run_outbox_dispatcher(
    session_factory: async_sessionmaker,
    outbox_model: Any,
    log,
    service_name: str,
) -> None:
    settings = get_settings()
    rabbit = RabbitMQClient()
    await rabbit.connect()
    log.info("outbox_dispatcher_started", service=service_name)

    try:
        while True:
            # BUG FIX: this iteration used to run with no try/except around it.
            # Any transient failure (a dropped DB connection, a momentary
            # RabbitMQ hiccup) would propagate out of the `while True` loop
            # entirely, and since this function runs as a fire-and-forget
            # asyncio.create_task() with nothing supervising/restarting it,
            # the ENTIRE service would silently stop publishing outbox events
            # forever after just one transient error. Catching here and
            # continuing (with a short backoff) means a blip causes a brief
            # delay in event delivery instead of a permanent outage.
            try:
                async with session_factory() as session:
                    result = await session.execute(
                        select(outbox_model)
                        .where(outbox_model.published.is_(False))
                        .order_by(outbox_model.created_at)
                        .limit(settings.outbox_batch_size)
                    )
                    rows = result.scalars().all()

                    for row in rows:
                        await rabbit.publish(row.routing_key, row.payload)
                        row.published = True
                        log.debug("outbox_published", message_id=row.id, routing_key=row.routing_key)

                    if rows:
                        await session.commit()

                await asyncio.sleep(settings.outbox_poll_interval_sec)
            except asyncio.CancelledError:
                raise  # let real shutdown/cancellation propagate, don't swallow it
            except Exception:
                log.exception("outbox_dispatcher_iteration_failed", service=service_name)
                await asyncio.sleep(settings.outbox_poll_interval_sec)
    finally:
        await rabbit.close()
