"""
Each service calls `make_engine_and_session(db_name)` with ITS OWN database
name (order_db / inventory_db / payment_db / saga_db / notification_db).
This enforces the "each service owns its own data" rule at the code level.

Note: there is deliberately NO shared `Base` class here (there used to be --
see git history / earlier phases -- but a single shared DeclarativeBase
meant that any script importing model modules from more than one service in
the same process, e.g. scripts/verify_consistency.py, would crash with
"Table 'outbox' is already defined for this MetaData instance", since
every service happens to have its own `outbox` table and they were all
registering into the SAME metadata registry. Each service's models.py now
defines its own local Base instead, so table names can safely repeat across
services -- which makes sense, since they really are entirely separate
databases with no relationship to each other.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from shared.settings import get_settings


def make_engine_and_session(db_name: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    settings = get_settings()
    engine = create_async_engine(settings.db_url(db_name), echo=False, pool_size=10, max_overflow=20)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return engine, session_factory
