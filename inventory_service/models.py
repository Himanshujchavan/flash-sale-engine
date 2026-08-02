"""
Inventory Service owns inventory_db, with two tables:

  inventory_items -- per-SKU available_qty / reserved_qty
  outbox          -- same transactional-outbox shape as order_service's,
                     but in THIS service's own database (never shared).

Note there is no cross-service foreign key to orders.id here on purpose --
services never reference another service's primary keys directly in SQL;
the only linkage is the order_id carried inside events/commands.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, CheckConstraint, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class InventoryItem(Base):
    __tablename__ = "inventory_items"
    __table_args__ = (
        CheckConstraint("available_qty >= 0", name="ck_available_qty_nonneg"),
        CheckConstraint("reserved_qty >= 0", name="ck_reserved_qty_nonneg"),
    )

    sku: Mapped[str] = mapped_column(String, primary_key=True)
    available_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class OutboxMessage(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    routing_key: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
