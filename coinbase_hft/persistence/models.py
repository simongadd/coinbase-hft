"""Data models for trade journal and session recording."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class TradeRecord:
    """Persisted record of a single order fill."""
    id: int | None
    session_id: str
    order_id: str
    client_order_id: str
    product_id: str
    side: str             # "buy" | "sell"
    order_type: str       # "market" | "limit"
    size: Decimal
    filled_size: Decimal
    avg_fill_price: Decimal
    limit_price: Decimal | None
    fee: Decimal
    mode: str             # "paper" | "live"
    status: str
    created_ts_ns: int
    filled_ts_ns: int
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionRecord:
    """High-level session metadata."""
    session_id: str
    mode: str
    strategy: str
    product_ids: list[str]
    start_ts: int
    end_ts: int | None
    initial_balance: dict[str, str]
    final_balance: dict[str, str]
    realized_pnl: str
    fees_paid: str
    trade_count: int
    notes: str = ""


CREATE_TRADES_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        TEXT NOT NULL,
    order_id          TEXT NOT NULL,
    client_order_id   TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    side              TEXT NOT NULL,
    order_type        TEXT NOT NULL,
    size              TEXT NOT NULL,
    filled_size       TEXT NOT NULL,
    avg_fill_price    TEXT NOT NULL,
    limit_price       TEXT,
    fee               TEXT NOT NULL,
    mode              TEXT NOT NULL,
    status            TEXT NOT NULL,
    created_ts_ns     INTEGER NOT NULL,
    filled_ts_ns      INTEGER NOT NULL,
    extra             TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_trades_session ON trades (session_id);
CREATE INDEX IF NOT EXISTS idx_trades_product ON trades (product_id, created_ts_ns);
"""

CREATE_SESSIONS_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    mode              TEXT NOT NULL,
    strategy          TEXT NOT NULL,
    product_ids       TEXT NOT NULL,
    start_ts          INTEGER NOT NULL,
    end_ts            INTEGER,
    initial_balance   TEXT NOT NULL,
    final_balance     TEXT DEFAULT '{}',
    realized_pnl      TEXT DEFAULT '0',
    fees_paid         TEXT DEFAULT '0',
    trade_count       INTEGER DEFAULT 0,
    notes             TEXT DEFAULT ''
);
"""

CREATE_MARKET_DATA_SQL = """
CREATE TABLE IF NOT EXISTS market_data_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    TEXT NOT NULL,
    product_id    TEXT NOT NULL,
    ts_ns         INTEGER NOT NULL,
    data_type     TEXT NOT NULL,  -- 'ticker' | 'trade' | 'candle'
    payload       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mds_session ON market_data_snapshots (session_id, product_id, ts_ns);
"""
