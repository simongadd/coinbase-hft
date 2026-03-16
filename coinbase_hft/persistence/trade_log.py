"""SQLite trade journal — records all fills in both paper and live mode."""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import aiosqlite

from coinbase_hft.persistence.models import (
    CREATE_SESSIONS_SQL,
    CREATE_TRADES_SQL,
    SessionRecord,
    TradeRecord,
)

logger = logging.getLogger(__name__)


class TradeLog:
    """Async SQLite-backed trade journal.

    Schema is identical in paper and live mode — strategies cannot tell the
    difference from the logged data structure alone.
    """

    def __init__(self, db_path: str | Path = "data/trades.db") -> None:
        self._db_path = Path(db_path)
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        for sql in CREATE_TRADES_SQL.split(";"):
            sql = sql.strip()
            if sql:
                await self._db.execute(sql)
        for sql in CREATE_SESSIONS_SQL.split(";"):
            sql = sql.strip()
            if sql:
                await self._db.execute(sql)
        await self._db.commit()
        logger.info("Trade log opened: %s", self._db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    async def log_trade(self, record: TradeRecord) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT INTO trades
               (session_id, order_id, client_order_id, product_id, side, order_type,
                size, filled_size, avg_fill_price, limit_price, fee, mode, status,
                created_ts_ns, filled_ts_ns, extra)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                record.session_id,
                record.order_id,
                record.client_order_id,
                record.product_id,
                record.side,
                record.order_type,
                str(record.size),
                str(record.filled_size),
                str(record.avg_fill_price),
                str(record.limit_price) if record.limit_price else None,
                str(record.fee),
                record.mode,
                record.status,
                record.created_ts_ns,
                record.filled_ts_ns,
                json.dumps(record.extra),
            ),
        )
        await self._db.commit()

    async def log_session(self, session: SessionRecord) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT OR REPLACE INTO sessions
               (session_id, mode, strategy, product_ids, start_ts, end_ts,
                initial_balance, final_balance, realized_pnl, fees_paid, trade_count, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                session.session_id,
                session.mode,
                session.strategy,
                json.dumps(session.product_ids),
                session.start_ts,
                session.end_ts,
                json.dumps(session.initial_balance),
                json.dumps(session.final_balance),
                session.realized_pnl,
                session.fees_paid,
                session.trade_count,
                session.notes,
            ),
        )
        await self._db.commit()

    async def get_session_trades(self, session_id: str) -> list[dict]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT * FROM trades WHERE session_id=? ORDER BY created_ts_ns",
            (session_id,),
        ) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    async def list_sessions(self) -> list[dict]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT * FROM sessions ORDER BY start_ts DESC"
        ) as cursor:
            return [dict(row) for row in await cursor.fetchall()]
