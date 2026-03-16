"""Unit tests for position tracking and PnL calculation."""

import pytest
from decimal import Decimal

from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.utils.decimal_math import ZERO


@pytest.fixture
def tracker() -> PositionTracker:
    return PositionTracker({"USD": Decimal("10000"), "BTC": Decimal("0")})


def test_initial_cash(tracker):
    assert tracker.cash("USD") == Decimal("10000")
    assert tracker.cash("BTC") == ZERO


def test_buy_updates_position(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("0.60"))
    pos = tracker.position("BTC-USD")
    assert pos.size == Decimal("1.0")
    assert pos.avg_entry_price == Decimal("100.00")


def test_buy_deducts_usd(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("0.60"))
    # 10000 - 100 (notional) - 0.60 (fee) = 9899.40
    assert tracker.cash("USD") == Decimal("9899.40")
    assert tracker.cash("BTC") == Decimal("1.0")


def test_sell_closes_long(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("0"))
    tracker.on_fill("BTC-USD", "sell", Decimal("1.0"), Decimal("110.00"), Decimal("0"))
    pos = tracker.position("BTC-USD")
    assert pos.size == ZERO
    assert pos.realized_pnl == Decimal("10.00")


def test_sell_partial_close(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("2.0"), Decimal("100.00"), Decimal("0"))
    tracker.on_fill("BTC-USD", "sell", Decimal("1.0"), Decimal("110.00"), Decimal("0"))
    pos = tracker.position("BTC-USD")
    assert pos.size == Decimal("1.0")
    assert pos.realized_pnl == Decimal("10.00")


def test_average_price_updated_on_accumulation(tracker):
    # Buy 1 BTC @ 100, then 1 more BTC @ 110 → avg should be 105
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("0"))
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("110.00"), Decimal("0"))
    pos = tracker.position("BTC-USD")
    assert pos.size == Decimal("2.0")
    assert pos.avg_entry_price == Decimal("105.00")


def test_unrealized_pnl(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("0"))
    pos = tracker.position("BTC-USD")
    upnl = pos.unrealized_pnl(Decimal("120.00"))
    assert upnl == Decimal("20.00")


def test_session_pnl_excludes_fees(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("1.0"), Decimal("100.00"), Decimal("1.00"))
    tracker.on_fill("BTC-USD", "sell", Decimal("1.0"), Decimal("110.00"), Decimal("1.00"))
    # Realized PnL = 10, fees = 2, net should be tracked
    assert tracker.session_realized_pnl == Decimal("10.00")
    assert tracker.session_fees == Decimal("2.00")
    assert tracker.session_net_pnl() == Decimal("8.00")


def test_snapshot_serializable(tracker):
    tracker.on_fill("BTC-USD", "buy", Decimal("0.5"), Decimal("50000"), Decimal("15"))
    snap = tracker.snapshot()
    assert "positions" in snap
    assert "cash" in snap
    assert "BTC-USD" in snap["positions"]
