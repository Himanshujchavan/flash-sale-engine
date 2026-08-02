"""
Payment Service owns payment_db, with two tables:

  payments -- one row per successful OR failed charge attempt (we record
              failures too, so there's an audit trail of declined cards etc.)
  outbox   -- same transactional-outbox shape used by every other service.

Note: payments.order_id is NOT a foreign key to order_service's orders
table -- there is no cross-database FK possible anyway (different Postgres
databases), and even if there were, we wouldn't use one. The order_id is
just a correlation field carried in from the ChargePayment command.
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


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    order_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)  # SUCCEEDED | FAILED | REFUNDED
    gateway_ref: Mapped[str] = mapped_column(String, nullable=False, default=_uuid)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class OutboxMessage(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    routing_key: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
