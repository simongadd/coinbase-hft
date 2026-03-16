"""Unit tests for decimal arithmetic helpers."""

import pytest
from decimal import Decimal

from coinbase_hft.utils.decimal_math import (
    apply_slippage_buy,
    apply_slippage_sell,
    bps_to_multiplier,
    fee_amount,
    mid_price,
    pnl,
    round_price,
    round_size,
    spread_bps,
    to_decimal,
    weighted_average_price,
    ZERO,
)


def test_to_decimal_from_string():
    assert to_decimal("100.00") == Decimal("100.00")


def test_to_decimal_from_float_no_precision_loss():
    # 0.1 + 0.2 == 0.3 in Decimal but not float
    val = to_decimal(0.1) + to_decimal(0.2)
    assert val == Decimal("0.3")


def test_bps_to_multiplier():
    assert bps_to_multiplier(10) == Decimal("0.001")
    assert bps_to_multiplier(100) == Decimal("0.01")
    assert bps_to_multiplier(0) == Decimal("0")


def test_apply_slippage_buy_increases_price():
    price = Decimal("100.00")
    result = apply_slippage_buy(price, 10)  # 10 bps = 0.1%
    assert result > price
    assert result == Decimal("100.10")


def test_apply_slippage_sell_decreases_price():
    price = Decimal("100.00")
    result = apply_slippage_sell(price, 10)
    assert result < price
    assert result == Decimal("99.90")


def test_fee_amount():
    notional = Decimal("1000.00")
    rate = Decimal("0.006")
    assert fee_amount(notional, rate) == Decimal("6.00000000")


def test_mid_price():
    assert mid_price(Decimal("100"), Decimal("102")) == Decimal("101")


def test_spread_bps():
    result = spread_bps(Decimal("99"), Decimal("101"))
    mid = Decimal("100")
    expected = (Decimal("2") / mid) * Decimal("10000")
    assert result == expected


def test_pnl_long_profitable():
    result = pnl(
        entry_price=Decimal("100"),
        exit_price=Decimal("110"),
        size=Decimal("1"),
        side="buy",
    )
    assert result == Decimal("10")


def test_pnl_long_loss():
    result = pnl(
        entry_price=Decimal("100"),
        exit_price=Decimal("90"),
        size=Decimal("1"),
        side="buy",
    )
    assert result == Decimal("-10")


def test_pnl_short_profitable():
    result = pnl(
        entry_price=Decimal("100"),
        exit_price=Decimal("90"),
        size=Decimal("1"),
        side="sell",
    )
    assert result == Decimal("10")


def test_weighted_average_price():
    fills = [
        (Decimal("100"), Decimal("1")),
        (Decimal("102"), Decimal("2")),
    ]
    # (100*1 + 102*2) / 3 = 304/3 ≈ 101.33...
    result = weighted_average_price(fills)
    assert result == Decimal("304") / Decimal("3")


def test_weighted_average_price_empty():
    assert weighted_average_price([]) == ZERO


def test_round_price():
    assert round_price(Decimal("100.123")) == Decimal("100.12")
    assert round_price(Decimal("100.125")) == Decimal("100.13")  # ROUND_HALF_UP


def test_round_size():
    assert round_size(Decimal("0.123456789")) == Decimal("0.12345679")
