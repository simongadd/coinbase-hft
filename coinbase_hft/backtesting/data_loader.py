"""Historical data loader for backtesting."""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from coinbase_hft.market_data.candle_aggregator import Candle
from coinbase_hft.utils.decimal_math import to_decimal

logger = logging.getLogger(__name__)


@dataclass
class HistoricalCandle:
    product_id: str
    open_time: int          # Unix timestamp seconds
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def to_candle(self, interval_seconds: int = 60) -> Candle:
        return Candle(
            product_id=self.product_id,
            interval_seconds=interval_seconds,
            open_time=self.open_time * 1_000_000_000,
            close_time=(self.open_time + interval_seconds) * 1_000_000_000,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            is_closed=True,
        )


def load_candles_csv(
    path: str | Path,
    product_id: str,
    interval_seconds: int = 60,
) -> list[HistoricalCandle]:
    """Load OHLCV candles from a CSV file.

    Expected columns: timestamp,open,high,low,close,volume
    (Coinbase Pro / Advanced Trade export format)
    """
    candles: list[HistoricalCandle] = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                candles.append(HistoricalCandle(
                    product_id=product_id,
                    open_time=int(row.get("timestamp", row.get("time", 0))),
                    open=to_decimal(row["open"]),
                    high=to_decimal(row["high"]),
                    low=to_decimal(row["low"]),
                    close=to_decimal(row["close"]),
                    volume=to_decimal(row["volume"]),
                ))
            except (KeyError, ValueError) as exc:
                logger.warning("Skipping malformed row in %s: %s", path, exc)
    candles.sort(key=lambda c: c.open_time)
    logger.info("Loaded %d candles for %s from %s", len(candles), product_id, path)
    return candles


def load_candles_json(path: str | Path, product_id: str) -> list[HistoricalCandle]:
    """Load from a JSON array: [{start, open, high, low, close, volume}]"""
    with open(path) as f:
        data = json.load(f)
    candles = []
    for row in data:
        candles.append(HistoricalCandle(
            product_id=product_id,
            open_time=int(row.get("start", row.get("timestamp", 0))),
            open=to_decimal(row["open"]),
            high=to_decimal(row["high"]),
            low=to_decimal(row["low"]),
            close=to_decimal(row["close"]),
            volume=to_decimal(row["volume"]),
        ))
    candles.sort(key=lambda c: c.open_time)
    return candles
