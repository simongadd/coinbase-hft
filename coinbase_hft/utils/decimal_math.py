"""Precise decimal arithmetic helpers — no floats for money."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

ZERO = Decimal("0")
ONE = Decimal("1")
BPS_DIVISOR = Decimal("10000")

# Coinbase uses 8 decimal places for crypto, 2 for USD
PRICE_PLACES = Decimal("0.01")
SIZE_PLACES = Decimal("0.00000001")
FEE_PLACES = Decimal("0.00000001")


def to_decimal(value: str | float | int | Decimal) -> Decimal:
    """Convert any numeric type to Decimal safely. Never pass floats for money."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        # Preserve exact string representation to avoid float imprecision
        return Decimal(str(value))
    return Decimal(str(value))


def round_price(price: Decimal) -> Decimal:
    return price.quantize(PRICE_PLACES, rounding=ROUND_HALF_UP)


def round_size(size: Decimal) -> Decimal:
    return size.quantize(SIZE_PLACES, rounding=ROUND_HALF_UP)


def round_fee(fee: Decimal) -> Decimal:
    return fee.quantize(FEE_PLACES, rounding=ROUND_HALF_UP)


def bps_to_multiplier(bps: int | Decimal) -> Decimal:
    """Convert basis points to a multiplier, e.g. 5 bps → 0.0005."""
    return to_decimal(bps) / BPS_DIVISOR


def apply_slippage_buy(price: Decimal, slippage_bps: int | Decimal) -> Decimal:
    """Apply positive slippage to a buy (worse price — you pay more)."""
    return price * (ONE + bps_to_multiplier(slippage_bps))


def apply_slippage_sell(price: Decimal, slippage_bps: int | Decimal) -> Decimal:
    """Apply positive slippage to a sell (worse price — you receive less)."""
    return price * (ONE - bps_to_multiplier(slippage_bps))


def notional_value(price: Decimal, size: Decimal) -> Decimal:
    return price * size


def fee_amount(notional: Decimal, fee_rate: Decimal) -> Decimal:
    return round_fee(notional * fee_rate)


def pnl(entry_price: Decimal, exit_price: Decimal, size: Decimal, side: str) -> Decimal:
    """Calculate raw PnL before fees. side must be 'buy' or 'sell'."""
    if side == "buy":
        return (exit_price - entry_price) * size
    return (entry_price - exit_price) * size


def mid_price(bid: Decimal, ask: Decimal) -> Decimal:
    return (bid + ask) / 2


def spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    if bid == ZERO:
        return ZERO
    return ((ask - bid) / mid_price(bid, ask)) * BPS_DIVISOR


def weighted_average_price(fills: list[tuple[Decimal, Decimal]]) -> Decimal:
    """Compute VWAP from a list of (price, size) tuples."""
    total_notional = sum(p * s for p, s in fills)
    total_size = sum(s for _, s in fills)
    if total_size == ZERO:
        return ZERO
    return total_notional / total_size
