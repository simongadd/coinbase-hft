"""Ring-buffer time-series cache for market data snapshots."""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Deque

from coinbase_hft.market_data.candle_aggregator import Candle
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


@dataclass
class TradeEvent:
    product_id: str
    price: Decimal
    size: Decimal
    side: str          # "buy" | "sell"
    trade_id: str
    ts_ns: int


@dataclass
class TickerSnapshot:
    product_id: str
    price: Decimal
    best_bid: Decimal
    best_ask: Decimal
    volume_24h: Decimal
    ts_ns: int


class MarketDataStore:
    """In-memory ring-buffer store for recent market data per product.

    Provides fast lookups for strategies: recent trades, candles, ticker state.
    """

    def __init__(self, max_trades: int = 1000, max_candles: int = 500) -> None:
        self._trades: dict[str, Deque[TradeEvent]] = {}
        self._candles: dict[str, dict[int, Deque[Candle]]] = {}  # product → interval → deque
        self._ticker: dict[str, TickerSnapshot] = {}
        self._max_trades = max_trades
        self._max_candles = max_candles
        self._latency_samples: deque[float] = deque(maxlen=200)  # ms, rolling window

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def add_trade(self, event: TradeEvent) -> None:
        pid = event.product_id
        if pid not in self._trades:
            self._trades[pid] = deque(maxlen=self._max_trades)
        self._trades[pid].append(event)

    def add_candle(self, candle: Candle) -> None:
        pid = candle.product_id
        iv = candle.interval_seconds
        if pid not in self._candles:
            self._candles[pid] = {}
        if iv not in self._candles[pid]:
            self._candles[pid][iv] = deque(maxlen=self._max_candles)
        self._candles[pid][iv].append(candle)

    def update_ticker(self, snapshot: TickerSnapshot) -> None:
        self._ticker[snapshot.product_id] = snapshot

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def recent_trades(self, product_id: str, n: int | None = None) -> list[TradeEvent]:
        trades = list(self._trades.get(product_id, []))
        return trades[-n:] if n else trades

    def candles(self, product_id: str, interval_seconds: int, n: int | None = None) -> list[Candle]:
        try:
            buf = list(self._candles[product_id][interval_seconds])
            return buf[-n:] if n else buf
        except KeyError:
            return []

    def ticker(self, product_id: str) -> TickerSnapshot | None:
        return self._ticker.get(product_id)

    def close_prices(self, product_id: str, interval_seconds: int, n: int) -> list[Decimal]:
        return [c.close for c in self.candles(product_id, interval_seconds, n)]

    def volumes(self, product_id: str, interval_seconds: int, n: int) -> list[Decimal]:
        return [c.volume for c in self.candles(product_id, interval_seconds, n)]

    def record_latency_sample(self, latency_ms: float) -> None:
        self._latency_samples.append(latency_ms)

    def latency_p95_ms(self) -> float:
        if not self._latency_samples:
            return 100.0  # conservative default
        samples = sorted(self._latency_samples)
        idx = int(len(samples) * 0.95)
        return float(samples[min(idx, len(samples) - 1)])

    def vwap(self, product_id: str, n_trades: int = 100) -> Decimal | None:
        """VWAP over the last N trades."""
        trades = self.recent_trades(product_id, n_trades)
        if not trades:
            return None
        total_notional = sum(t.price * t.size for t in trades)
        total_volume = sum(t.size for t in trades)
        if total_volume == ZERO:
            return None
        return total_notional / total_volume
