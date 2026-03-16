"""Unit tests for the L2 order book."""

import pytest
from decimal import Decimal

from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import ZERO


@pytest.fixture
def empty_book() -> OrderBook:
    return OrderBook("BTC-USD")


@pytest.fixture
def filled_book() -> OrderBook:
    book = OrderBook("BTC-USD")
    book.apply_snapshot(
        bids=[["99.50", "1.0"], ["99.00", "2.0"], ["98.00", "5.0"]],
        asks=[["100.00", "1.5"], ["100.50", "2.5"], ["101.00", "3.0"]],
    )
    return book


def test_snapshot_initializes_book(filled_book):
    assert filled_book.initialized
    assert filled_book.best_bid == Decimal("99.50")
    assert filled_book.best_ask == Decimal("100.00")


def test_mid_price(filled_book):
    expected = (Decimal("99.50") + Decimal("100.00")) / 2
    assert filled_book.mid == expected


def test_spread(filled_book):
    assert filled_book.spread == Decimal("0.50")


def test_not_crossed(filled_book):
    assert not filled_book.is_crossed()


def test_apply_delta_update(filled_book):
    filled_book.apply_delta("bid", "99.50", "3.0")
    assert filled_book.bid_size_at(Decimal("99.50")) == Decimal("3.0")


def test_apply_delta_remove(filled_book):
    filled_book.apply_delta("bid", "99.50", "0")
    assert filled_book.bid_size_at(Decimal("99.50")) == ZERO
    assert filled_book.best_bid == Decimal("99.00")


def test_apply_delta_new_level(filled_book):
    filled_book.apply_delta("bid", "99.75", "0.5")
    assert filled_book.best_bid == Decimal("99.75")


def test_crossed_book():
    book = OrderBook("BTC-USD")
    book.apply_snapshot(
        bids=[["101.00", "1.0"]],
        asks=[["100.00", "1.0"]],
    )
    assert book.is_crossed()


def test_bids_sorted_descending(filled_book):
    bids = filled_book.bids()
    prices = [b.price for b in bids]
    assert prices == sorted(prices, reverse=True)


def test_asks_sorted_ascending(filled_book):
    asks = filled_book.asks()
    prices = [a.price for a in asks]
    assert prices == sorted(prices)


def test_simulate_market_buy(filled_book):
    fills = filled_book.simulate_market_buy(Decimal("2.5"))
    assert len(fills) > 0
    total_filled = sum(s for _, s in fills)
    assert total_filled == Decimal("2.5")


def test_simulate_market_buy_partial_liquidity(filled_book):
    # Request more than available
    fills = filled_book.simulate_market_buy(Decimal("100.0"))
    total_filled = sum(s for _, s in fills)
    # Should fill up to total ask depth
    total_ask_depth = sum(a.size for a in filled_book.asks())
    assert total_filled == total_ask_depth


def test_order_imbalance_positive_when_more_bids():
    book = OrderBook("BTC-USD")
    book.apply_snapshot(
        bids=[["99", "10.0"]],
        asks=[["100", "2.0"]],
    )
    imbalance = book.order_imbalance()
    assert imbalance > ZERO


def test_empty_book_properties(empty_book):
    assert empty_book.best_bid is None
    assert empty_book.best_ask is None
    assert empty_book.mid is None
    assert not empty_book.initialized
