"""Real-time OHLCV candle builder from the trade stream."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


@dataclass
class Candle:
    product_id: str
    interval_seconds: int
    open_time: int          # Unix timestamp ns of the candle open
    close_time: int         # Unix timestamp ns of the candle close (exclusive)
    open: Decimal = ZERO
    high: Decimal = ZERO
    low: Decimal = ZERO
    close: Decimal = ZERO
    volume: Decimal = ZERO
    trade_count: int = 0
    is_closed: bool = False

    def update(self, price: Decimal, size: Decimal) -> None:
        if self.open == ZERO:
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.trade_count += 1

    def to_dict(self) -> dict:
        return {
            "product_id": self.product_id,
            "interval_seconds": self.interval_seconds,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "trade_count": self.trade_count,
        }


class CandleAggregator:
    """Aggregate trade events into fixed-interval OHLCV candles.

    Emits a completed Candle whenever the current interval rolls over.
    """

    def __init__(
        self,
        product_id: str,
        interval_seconds: int = 60,
        on_candle_closed: None = None,
    ) -> None:
        self.product_id = product_id
        self.interval_ns = interval_seconds * 1_000_000_000
        self.interval_seconds = interval_seconds
        self._on_candle_closed = on_candle_closed  # async callback(Candle)
        self._current: Candle | None = None

    def _candle_open_time(self, ts_ns: int) -> int:
        """Round down timestamp to the nearest interval boundary."""
        interval_ns = self.interval_ns
        return (ts_ns // interval_ns) * interval_ns

    async def on_trade(self, price_str: str, size_str: str, ts_ns: int) -> Candle | None:
        """Process a trade. Returns a closed candle if the interval rolled, else None."""
        price = to_decimal(price_str)
        size = to_decimal(size_str)
        open_time = self._candle_open_time(ts_ns)
        close_time = open_time + self.interval_ns

        closed_candle: Candle | None = None

        if self._current is None:
            self._current = Candle(
                product_id=self.product_id,
                interval_seconds=self.interval_seconds,
                open_time=open_time,
                close_time=close_time,
            )
        elif ts_ns >= self._current.close_time:
            # Candle rolled — close current and start new one
            self._current.is_closed = True
            closed_candle = self._current
            logger.debug(
                "Candle closed %s O=%s H=%s L=%s C=%s V=%s",
                self.product_id,
                closed_candle.open, closed_candle.high,
                closed_candle.low, closed_candle.close,
                closed_candle.volume,
            )
            self._current = Candle(
                product_id=self.product_id,
                interval_seconds=self.interval_seconds,
                open_time=open_time,
                close_time=close_time,
            )
            if self._on_candle_closed:
                await self._on_candle_closed(closed_candle)

        self._current.update(price, size)
        return closed_candle

    @property
    def current_candle(self) -> Candle | None:
        return self._current
