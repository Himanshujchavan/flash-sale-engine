"""
Notification Service owns notification_db, with a single table.

Unlike every other service, this one has NO outbox table -- it's a
TERMINAL consumer at the end of the chain (OrderConfirmed / OrderCancelled
are the last events in the saga's lifecycle). It never needs to publish
anything further, so the whole transactional-outbox machinery that exists
specifically to make "commit DB write + publish event" atomic simply
doesn't apply here: there's nothing to publish.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    order_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    notification_type: Mapped[str] = mapped_column(String, nullable=False)  # ORDER_CONFIRMED | ORDER_CANCELLED
    message: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
