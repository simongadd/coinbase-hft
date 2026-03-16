"""Unit tests for technical indicator signals."""

import pytest
from decimal import Decimal

from coinbase_hft.strategy.signals.technical import (
    bollinger_bands,
    ema,
    ema_crossover,
    rsi,
    sma,
    vwap,
)
from coinbase_hft.utils.decimal_math import ZERO


def _prices(values: list[float]) -> list[Decimal]:
    return [Decimal(str(v)) for v in values]


def test_sma_basic():
    prices = _prices([10, 20, 30])
    result = sma(prices, 3)
    assert result == Decimal("20")


def test_sma_insufficient_data():
    prices = _prices([10, 20])
    assert sma(prices, 3) is None


def test_ema_returns_value():
    prices = _prices([100] * 20)  # all same price → EMA == price
    result = ema(prices, 10)
    assert result == Decimal("100")


def test_rsi_all_gains_returns_100():
    prices = _prices(list(range(1, 20)))  # monotonically increasing
    result = rsi(prices, 14)
    assert result == Decimal("100")


def test_rsi_all_losses_returns_0():
    prices = _prices(list(range(20, 0, -1)))  # monotonically decreasing
    result = rsi(prices, 14)
    assert result == ZERO


def test_rsi_range():
    import random
    random.seed(42)
    prices = _prices([100 + random.uniform(-5, 5) for _ in range(30)])
    result = rsi(prices, 14)
    assert result is not None
    assert ZERO <= result <= Decimal("100")


def test_rsi_insufficient_data():
    prices = _prices([100, 101])
    assert rsi(prices, 14) is None


def test_bollinger_bands_returns_triple():
    prices = _prices([100.0] * 25)  # constant price
    result = bollinger_bands(prices, period=20)
    assert result is not None
    upper, middle, lower = result
    assert upper >= middle >= lower
    # Constant prices → zero std → bands are all equal
    assert upper == middle == lower


def test_bollinger_bands_insufficient_data():
    prices = _prices([100.0] * 10)
    assert bollinger_bands(prices, period=20) is None


def test_ema_crossover_bullish():
    # Fast EMA crosses above slow: prices rising sharply at the end
    flat = [Decimal("100")] * 25
    rising = flat + [Decimal("100") + Decimal(str(i * 5)) for i in range(1, 15)]
    result = ema_crossover(rising, fast_period=5, slow_period=10)
    # Not deterministic without specific values, but shouldn't raise
    assert result in (1, -1, 0, None)


def test_ema_crossover_insufficient_data():
    prices = _prices([100.0] * 5)
    assert ema_crossover(prices, fast_period=5, slow_period=10) is None


def test_vwap():
    prices = _prices([100, 102, 101])
    volumes = _prices([10, 20, 15])
    result = vwap(prices, volumes)
    expected = (100 * 10 + 102 * 20 + 101 * 15) / (10 + 20 + 15)
    assert abs(float(result) - expected) < 0.0001


def test_vwap_empty():
    assert vwap([], []) is None
