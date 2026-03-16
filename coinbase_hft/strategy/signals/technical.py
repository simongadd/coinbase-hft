"""Technical indicator signals: RSI, VWAP, Bollinger Bands, EMA crossovers."""

from __future__ import annotations

from decimal import Decimal

from coinbase_hft.utils.decimal_math import ZERO, to_decimal

D = Decimal


def ema(prices: list[Decimal], period: int) -> Decimal | None:
    """Exponential moving average."""
    if len(prices) < period:
        return None
    k = D(2) / (D(period) + D(1))
    ema_val = sum(prices[:period]) / D(period)
    for price in prices[period:]:
        ema_val = price * k + ema_val * (D(1) - k)
    return ema_val


def sma(prices: list[Decimal], period: int) -> Decimal | None:
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / D(period)


def rsi(prices: list[Decimal], period: int = 14) -> Decimal | None:
    """Relative Strength Index (0–100)."""
    if len(prices) < period + 1:
        return None
    gains: list[Decimal] = []
    losses: list[Decimal] = []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        if delta > ZERO:
            gains.append(delta)
            losses.append(ZERO)
        else:
            gains.append(ZERO)
            losses.append(abs(delta))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[-period:]) / D(period)
    avg_loss = sum(losses[-period:]) / D(period)

    if avg_loss == ZERO:
        return D(100)
    rs = avg_gain / avg_loss
    return D(100) - (D(100) / (D(1) + rs))


def bollinger_bands(
    prices: list[Decimal],
    period: int = 20,
    num_std: Decimal = D("2"),
) -> tuple[Decimal, Decimal, Decimal] | None:
    """Return (upper, middle, lower) Bollinger Bands."""
    if len(prices) < period:
        return None
    window = prices[-period:]
    middle = sum(window) / D(period)
    variance = sum((p - middle) ** 2 for p in window) / D(period)
    std = variance.sqrt()
    return middle + num_std * std, middle, middle - num_std * std


def ema_crossover(
    prices: list[Decimal],
    fast_period: int = 9,
    slow_period: int = 21,
) -> int | None:
    """
    Return:
     1  — fast EMA crossed above slow (bullish)
    -1  — fast EMA crossed below slow (bearish)
     0  — no crossover
    None — insufficient data
    """
    if len(prices) < slow_period + 2:
        return None
    fast_now = ema(prices, fast_period)
    slow_now = ema(prices, slow_period)
    fast_prev = ema(prices[:-1], fast_period)
    slow_prev = ema(prices[:-1], slow_period)

    if None in (fast_now, slow_now, fast_prev, slow_prev):
        return None

    assert fast_now is not None and slow_now is not None
    assert fast_prev is not None and slow_prev is not None

    was_below = fast_prev < slow_prev
    now_above = fast_now > slow_now
    was_above = fast_prev > slow_prev
    now_below = fast_now < slow_now

    if was_below and now_above:
        return 1
    if was_above and now_below:
        return -1
    return 0


def vwap(prices: list[Decimal], volumes: list[Decimal]) -> Decimal | None:
    """Volume-weighted average price."""
    if not prices or len(prices) != len(volumes):
        return None
    total_vol = sum(volumes)
    if total_vol == ZERO:
        return None
    return sum(p * v for p, v in zip(prices, volumes)) / total_vol
