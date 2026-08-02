"""
Saga Coordinator owns saga_db, with two tables:

  saga_state -- one row per order_id, tracking where that order currently
                sits in the Reserve -> Charge -> Confirm/Compensate
                lifecycle (see shared/events.py's SagaState constants).
  outbox     -- same transactional-outbox shape as every other service.

The coordinator does NOT own orders, inventory, or payments data -- it only
tracks its OWN view of the workflow (current state + the handful of fields
it needs to issue the next command), correlated purely by order_id.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class SagaState(Base):
    __tablename__ = "saga_state"

    order_id: Mapped[str] = mapped_column(String, primary_key=True)
    state: Mapped[str] = mapped_column(String, nullable=False)

    # Carried along from OrderCreated so later steps (ReserveInventory,
    # ChargePayment, ReleaseInventory) have what they need without having
    # to query the Order Service directly (services never call each other
    # synchronously in this architecture).
    sku: Mapped[str] = mapped_column(String, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    payment_id: Mapped[str | None] = mapped_column(String, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class OutboxMessage(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    routing_key: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
