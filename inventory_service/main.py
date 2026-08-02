"""
Inventory Service -- run with:
    uvicorn inventory_service.main:app --reload --port 8002

Responsibilities:
  1. Admin/demo HTTP endpoints -- seed stock, check current stock (there is
     no public "reserve" HTTP endpoint; reservation only happens via the
     ReserveInventory command from the Saga Coordinator, over RabbitMQ).
  2. RabbitMQ consumer -- ReserveInventory -> try_reserve() -> publishes
     InventoryReserved or InventoryReservationFailed.
                           ReleaseInventory -> release_reservation() ->
     publishes InventoryReleased. (Compensation path, triggered when a
     later saga step fails, e.g. payment declined.)
  3. Background outbox dispatcher (shared/outbox.py).
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from inventory_service.models import Base, InventoryItem, OutboxMessage
from inventory_service.reservation import release_reservation, try_reserve
from shared.db import make_engine_and_session
from shared.events import (
    InventoryReleased,
    InventoryReservationFailed,
    InventoryReserved,
    ReleaseInventory,
    ReserveInventory,
)
from shared.logging_config import configure_logging
from shared.outbox import run_outbox_dispatcher
from shared.rabbitmq import RabbitMQClient
from shared.redis_client import IdempotencyStore, TokenBucketLimiter

log = configure_logging("inventory_service")

engine, SessionLocal = make_engine_and_session("inventory_db")
rabbit_consumer = RabbitMQClient()
rate_limiter = TokenBucketLimiter()
reservation_idempotency = IdempotencyStore()

app = FastAPI(title="Inventory Service")

_background_tasks: list[asyncio.Task] = []


class SeedRequest(BaseModel):
    sku: str
    qty: int = Field(gt=0)


class StockResponse(BaseModel):
    sku: str
    available_qty: int
    reserved_qty: int


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await rabbit_consumer.connect()
    _background_tasks.append(
        asyncio.create_task(run_outbox_dispatcher(SessionLocal, OutboxMessage, log, "inventory_service"))
    )
    _background_tasks.append(asyncio.create_task(_consume_saga_commands()))
    log.info("inventory_service_started")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in _background_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await rabbit_consumer.close()
    await engine.dispose()


@app.post("/inventory/seed", response_model=StockResponse)
async def seed_inventory(req: SeedRequest) -> StockResponse:
    """Demo/admin helper: create a SKU or add to its available stock."""
    async with SessionLocal() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.sku == req.sku))
        item = result.scalar_one_or_none()
        if item is None:
            item = InventoryItem(sku=req.sku, available_qty=req.qty, reserved_qty=0)
            session.add(item)
        else:
            item.available_qty += req.qty
        await session.commit()
        return StockResponse(sku=item.sku, available_qty=item.available_qty, reserved_qty=item.reserved_qty)


@app.get("/inventory/{sku}", response_model=StockResponse)
async def get_stock(sku: str) -> StockResponse:
    async with SessionLocal() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.sku == sku))
        item = result.scalar_one_or_none()
        if item is None:
            raise HTTPException(status_code=404, detail="sku not found")
        return StockResponse(sku=item.sku, available_qty=item.available_qty, reserved_qty=item.reserved_qty)


async def _consume_saga_commands() -> None:
    async def handle(routing_key: str, payload: dict) -> None:
        async with SessionLocal() as session:
            if routing_key == ReserveInventory.__name__:
                cmd = ReserveInventory.model_validate(payload)
                result = await try_reserve(
                    session, cmd.order_id, cmd.sku, cmd.qty, rate_limiter, reservation_idempotency
                )

                if result.success:
                    event = InventoryReserved(order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty)
                    log.info("inventory_reserved", order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty)
                else:
                    event = InventoryReservationFailed(
                        order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty, reason=result.reason or "unknown",
                    )
                    log.warning(
                        "inventory_reservation_failed",
                        order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty, reason=result.reason,
                    )

                session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
                await session.commit()

            elif routing_key == ReleaseInventory.__name__:
                cmd = ReleaseInventory.model_validate(payload)
                await release_reservation(session, cmd.order_id, cmd.sku, cmd.qty, reservation_idempotency)

                event = InventoryReleased(order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty)
                session.add(OutboxMessage(routing_key=event.routing_key, payload=event.model_dump(mode="json")))
                await session.commit()
                log.info("inventory_released", order_id=cmd.order_id, sku=cmd.sku, qty=cmd.qty)

    await rabbit_consumer.consume(
        queue_name="inventory_service.saga_commands",
        routing_keys=[ReserveInventory.__name__, ReleaseInventory.__name__],
        handler=handle,
    )
