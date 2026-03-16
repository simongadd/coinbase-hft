"""Integration tests for the full paper trading loop."""

import pytest
from decimal import Decimal

from coinbase_hft.core.clock import SimulatedClock
from coinbase_hft.core.event_bus import EventBus, EventType
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor, OrderStatus
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import ZERO


@pytest.fixture
def setup():
    clock = SimulatedClock(start_ns=1_000_000_000_000)
    bus = EventBus()
    positions = PositionTracker({"USD": Decimal("10000"), "BTC": Decimal("0")})
    fill_model = FillModel(
        slippage_bps=0,
        fill_probability=Decimal("1.0"),
        fee_rate=Decimal("0.006"),
        queue_position_factor=Decimal("0.0"),
    )
    paper_exec = PaperExecutor(fill_model, positions, bus, clock)
    order_mgr = OrderManager(
        mode="paper",
        event_bus=bus,
        position_tracker=positions,
        clock=clock,
        paper_executor=paper_exec,
    )
    book = OrderBook("BTC-USD")
    book.apply_snapshot(
        bids=[["99.50", "10.0"]],
        asks=[["100.00", "10.0"]],
    )
    return order_mgr, positions, bus, clock, book


@pytest.mark.asyncio
async def test_market_buy_updates_position(setup):
    order_mgr, positions, bus, clock, book = setup
    order = await order_mgr.submit_order(
        product_id="BTC-USD",
        side="buy",
        order_type="market",
        size=Decimal("1.0"),
        book=book,
    )
    assert order is not None
    assert order.status == OrderStatus.FILLED
    pos = positions.position("BTC-USD")
    assert pos.size == Decimal("1.0")


@pytest.mark.asyncio
async def test_market_buy_deducts_usd(setup):
    order_mgr, positions, bus, clock, book = setup
    await order_mgr.submit_order(
        product_id="BTC-USD",
        side="buy",
        order_type="market",
        size=Decimal("1.0"),
        book=book,
    )
    # Should have spent ~100 USD + 0.6 fee
    usd = positions.cash("USD")
    assert usd < Decimal("10000")


@pytest.mark.asyncio
async def test_limit_order_fills_on_book_update(setup):
    order_mgr, positions, bus, clock, book = setup
    order = await order_mgr.submit_order(
        product_id="BTC-USD",
        side="buy",
        order_type="limit",
        size=Decimal("0.5"),
        limit_price=Decimal("100.00"),
    )
    assert order is not None
    assert order.status == OrderStatus.OPEN

    # Trigger book update which should fill the limit order
    await order_mgr.on_book_update("BTC-USD", book)
    assert order.status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_limit_order_does_not_fill_when_price_too_low(setup):
    order_mgr, positions, bus, clock, book = setup
    order = await order_mgr.submit_order(
        product_id="BTC-USD",
        side="buy",
        order_type="limit",
        size=Decimal("0.5"),
        limit_price=Decimal("90.00"),  # way below best ask
    )
    await order_mgr.on_book_update("BTC-USD", book)
    assert order.status == OrderStatus.OPEN  # not filled


@pytest.mark.asyncio
async def test_buy_then_sell_calculates_pnl(setup):
    order_mgr, positions, bus, clock, book = setup
    # Buy at 100
    await order_mgr.submit_order(
        product_id="BTC-USD", side="buy", order_type="market",
        size=Decimal("1.0"), book=book,
    )
    # Update book: price goes to 110
    book2 = OrderBook("BTC-USD")
    book2.apply_snapshot(
        bids=[["109.00", "10.0"]],
        asks=[["110.00", "10.0"]],
    )
    # Sell at 109 (best bid)
    await order_mgr.submit_order(
        product_id="BTC-USD", side="sell", order_type="market",
        size=Decimal("1.0"), book=book2,
    )
    # Should have ~9 USD profit (109 - 100) minus fees
    assert positions.session_realized_pnl > ZERO


@pytest.mark.asyncio
async def test_cancel_all_orders(setup):
    order_mgr, positions, bus, clock, book = setup
    # Submit 3 limit orders that won't fill
    for i in range(3):
        await order_mgr.submit_order(
            product_id="BTC-USD", side="buy", order_type="limit",
            size=Decimal("0.1"), limit_price=Decimal("50.00"),
        )
    open_before = len(order_mgr.open_orders("BTC-USD"))
    assert open_before == 3
    count = await order_mgr.cancel_all_orders("BTC-USD")
    assert count == 3
    open_after = len(order_mgr.open_orders("BTC-USD"))
    assert open_after == 0


@pytest.mark.asyncio
async def test_idempotent_order_rejected(setup):
    order_mgr, positions, bus, clock, book = setup
    order1 = await order_mgr.submit_order(
        product_id="BTC-USD", side="buy", order_type="market",
        size=Decimal("0.1"), client_order_id="test-cid-1", book=book,
    )
    # Same client_order_id — should be rejected
    order2 = await order_mgr.submit_order(
        product_id="BTC-USD", side="buy", order_type="market",
        size=Decimal("0.1"), client_order_id="test-cid-1", book=book,
    )
    assert order1 is not None
    assert order2 is None
