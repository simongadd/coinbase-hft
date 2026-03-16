"""Unit tests for the paper trading fill simulation model."""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock

from coinbase_hft.execution.fill_model import FillModel, FillResult, OrderSide, OrderType
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import ZERO


@pytest.fixture
def book() -> OrderBook:
    b = OrderBook("BTC-USD")
    b.apply_snapshot(
        bids=[["99.50", "5.0"], ["99.00", "10.0"]],
        asks=[["100.00", "5.0"], ["100.50", "10.0"]],
    )
    return b


@pytest.fixture
def fill_model() -> FillModel:
    return FillModel(
        slippage_bps=10,
        fill_probability=Decimal("1.0"),  # always fill in tests
        fee_rate=Decimal("0.006"),
        queue_position_factor=Decimal("0.0"),  # no queue penalty in tests
    )


def test_market_buy_fills_from_ask(fill_model, book):
    result = fill_model.simulate_market_fill(
        order_id="test-1",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        book=book,
    )
    assert result.is_filled
    assert result.filled_size == Decimal("1.0")
    # Fill price should be ask + slippage
    assert result.avg_fill_price > Decimal("100.00")


def test_market_sell_fills_from_bid(fill_model, book):
    result = fill_model.simulate_market_fill(
        order_id="test-2",
        side=OrderSide.SELL,
        size=Decimal("1.0"),
        book=book,
    )
    assert result.is_filled
    assert result.filled_size == Decimal("1.0")
    # Fill price should be bid - slippage
    assert result.avg_fill_price < Decimal("99.50")


def test_market_order_fee_calculated(fill_model, book):
    result = fill_model.simulate_market_fill(
        order_id="test-3",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        book=book,
    )
    expected_fee = result.avg_fill_price * result.filled_size * Decimal("0.006")
    assert abs(result.fee - expected_fee) < Decimal("0.00000001")


def test_limit_buy_fills_when_ask_crosses(fill_model, book):
    result = fill_model.simulate_limit_fill(
        order_id="test-4",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        limit_price=Decimal("100.00"),
        book=book,
    )
    assert result.is_filled


def test_limit_buy_no_fill_when_ask_above_limit(fill_model, book):
    result = fill_model.simulate_limit_fill(
        order_id="test-5",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        limit_price=Decimal("99.00"),  # below best ask of 100.00
        book=book,
    )
    assert not result.is_filled


def test_limit_sell_fills_when_bid_crosses(fill_model, book):
    result = fill_model.simulate_limit_fill(
        order_id="test-6",
        side=OrderSide.SELL,
        size=Decimal("1.0"),
        limit_price=Decimal("99.50"),
        book=book,
    )
    assert result.is_filled


def test_limit_sell_no_fill_when_bid_below_limit(fill_model, book):
    result = fill_model.simulate_limit_fill(
        order_id="test-7",
        side=OrderSide.SELL,
        size=Decimal("1.0"),
        limit_price=Decimal("101.00"),  # above best bid of 99.50
        book=book,
    )
    assert not result.is_filled


def test_fill_probability_zero_never_fills(book):
    model = FillModel(fill_probability=Decimal("0.0"))
    result = model.simulate_limit_fill(
        order_id="test-8",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        limit_price=Decimal("100.00"),
        book=book,
    )
    assert not result.is_filled


def test_partial_fill_when_size_exceeds_liquidity(book):
    """Request 20 BTC when only 5 is available at best ask."""
    model = FillModel(
        fill_probability=Decimal("1.0"),
        queue_position_factor=Decimal("0.0"),
    )
    result = model.simulate_market_fill(
        order_id="test-9",
        side=OrderSide.BUY,
        size=Decimal("20.0"),
        book=book,
    )
    assert result.is_partial
    assert result.filled_size < Decimal("20.0")
