from __future__ import annotations

from cross_market_monitor.infrastructure.storage.sqlite_base import SQLiteRepositoryBase
from cross_market_monitor.infrastructure.storage.sqlite_query_repo import SQLiteQueryRepoMixin
from cross_market_monitor.infrastructure.storage.sqlite_state_repo import SQLiteStateRepoMixin
from cross_market_monitor.infrastructure.storage.sqlite_writer import SQLiteWriterMixin


class SQLiteRepository(
    SQLiteWriterMixin,
    SQLiteStateRepoMixin,
    SQLiteQueryRepoMixin,
    SQLiteRepositoryBase,
):
    pass
