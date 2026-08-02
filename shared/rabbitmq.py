"""
Thin wrapper around aio-pika so every service talks to RabbitMQ the same way.

Design:
  - One topic exchange ("flash_sale_events") shared by all services.
  - Each service declares its OWN queue and binds it to the routing keys
    (event/command names) it cares about. This is standard pub/sub fan-out:
    the exchange doesn't know or care who's listening.
  - Publishing is done from the Outbox Dispatcher (never directly from a
    request handler) -- see order_service/outbox.py for why.
"""
from __future__ import annotations

import json
import logging
from typing import Awaitable, Callable

import aio_pika
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage

from shared.settings import get_settings

logger = logging.getLogger(__name__)


class RabbitMQClient:
    def __init__(self, url: str | None = None, exchange_name: str | None = None):
        settings = get_settings()
        self.url = url or settings.rabbitmq_url
        self.exchange_name = exchange_name or settings.events_exchange
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self.url)
        await self._open_channel()
        logger.info("Connected to RabbitMQ, exchange=%s", self.exchange_name)

    async def _open_channel(self) -> None:
        """(Re)declares this client's channel and exchange against the current
        connection. Split out from connect() so publish() can call it again
        after a disconnect without redoing the whole connect_robust() dance."""
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=20)
        self._exchange = await self._channel.declare_exchange(
            self.exchange_name, ExchangeType.TOPIC, durable=True
        )

    async def close(self) -> None:
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                logger.debug("Ignoring error closing an already-broken connection", exc_info=True)

    async def publish(self, routing_key: str, payload: dict) -> None:
        """Publish a single message. Messages are persistent (survive broker restart)."""
        if not self._exchange:
            raise RuntimeError("RabbitMQClient.connect() must be called before publish()")
        body = json.dumps(payload, default=str).encode("utf-8")
        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=payload.get("message_id"),
        )

        try:
            await self._exchange.publish(message, routing_key=routing_key)
        except Exception:
            # BUG FIX (layer 1): aio_pika's connect_robust() transparently
            # reconnects the underlying TCP connection after a broker
            # restart/network blip, but a CHANNEL (and any exchange declared
            # on it) that was open at the moment of disconnect is left
            # permanently closed -- it is not swapped out for a fresh one
            # automatically. Re-declare the channel/exchange and retry once.
            logger.warning("Publish failed, re-opening channel and retrying once: %s", routing_key)
            try:
                await self._open_channel()
                await self._exchange.publish(message, routing_key=routing_key)
            except Exception:
                # BUG FIX (layer 2): observed in testing -- after a
                # `rabbitmqctl stop_app` / `start_app` cycle, the existing
                # RobustConnection object can itself remain stuck reporting
                # "Connection was not opened" indefinitely (25s+), i.e. its
                # own internal reconnect logic doesn't recover from this
                # particular failure mode. Rather than fail forever, fall
                # back to tearing down and re-establishing the connection
                # from scratch via connect(). If the broker is genuinely
                # still unreachable this will also fail, and the caller's
                # own retry loop (the outbox dispatcher's per-iteration
                # try/except) takes over on the next poll cycle.
                logger.warning("Channel reopen failed too, forcing full reconnect: %s", routing_key)
                await self.close()
                await self.connect()
                await self._exchange.publish(message, routing_key=routing_key)

        logger.debug("Published %s: %s", routing_key, payload.get("message_id"))

    async def consume(
        self,
        queue_name: str,
        routing_keys: list[str],
        handler: Callable[[str, dict], Awaitable[None]],
    ) -> None:
        """
        Declare a durable queue, bind it to the given routing keys on the shared
        exchange, and consume forever, calling `handler(routing_key, payload)`
        for each message. Messages are acked only after the handler succeeds --
        if the handler raises, the message is nacked and requeued, which is what
        gives us "at-least-once" processing (hence idempotency requirements on
        the consumer side).
        """
        if not self._channel or not self._exchange:
            raise RuntimeError("RabbitMQClient.connect() must be called before consume()")

        queue = await self._channel.declare_queue(queue_name, durable=True)
        for key in routing_keys:
            await queue.bind(self._exchange, routing_key=key)

        async def _on_message(message: AbstractIncomingMessage) -> None:
            routing_key = message.routing_key or ""
            payload = json.loads(message.body.decode("utf-8"))
            try:
                await handler(routing_key, payload)
                await message.ack()
            except Exception:
                logger.exception("Handler failed for %s, nacking for requeue", routing_key)
                await message.nack(requeue=True)

        await queue.consume(_on_message)
        logger.info("Consuming queue=%s bound to keys=%s", queue_name, routing_keys)
