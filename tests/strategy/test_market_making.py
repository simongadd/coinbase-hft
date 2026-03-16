"""Integration tests for the market-making strategy."""

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from coinbase_hft.core.clock import SimulatedClock
from coinbase_hft.core.event_bus import EventBus
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.market_data_store import MarketDataStore, TickerSnapshot
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.strategy.base_strategy import StrategyContext
from coinbase_hft.strategy.examples.market_making import MarketMakingStrategy


@pytest.fixture
def strategy_setup():
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
    store = MarketDataStore()
    order_mgr = OrderManager(
        mode="paper",
        event_bus=bus,
        position_tracker=positions,
        clock=clock,
        paper_executor=paper_exec,
    )
    strategy = MarketMakingStrategy(
        product_ids=["BTC-USD"],
        order_manager=order_mgr,
        data_store=store,
        clock=clock,
        config={
            "spread_bps": 20,
            "inventory_skew": False,
            "max_inventory": "1.0",
            "order_size": "0.01",
            "min_spread_bps": 1,
        },
    )
    book = OrderBook("BTC-USD")
    book.apply_snapshot(
        bids=[["99.90", "5.0"]],
        asks=[["100.10", "5.0"]],
    )
    store.update_ticker(TickerSnapshot(
        product_id="BTC-USD",
        price=Decimal("100.00"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.10"),
        volume_24h=Decimal("1000"),
        ts_ns=clock.now_ns(),
    ))
    return strategy, order_mgr, book, clock, positions


@pytest.mark.asyncio
async def test_market_making_posts_quotes(strategy_setup):
    strategy, order_mgr, book, clock, positions = strategy_setup
    await strategy.on_start()
    ctx = StrategyContext(
        product_id="BTC-USD",
        book=book,
        data_store=strategy._store,
        ts_ns=clock.now_ns(),
    )
    await strategy.on_tick(ctx)
    open_orders = order_mgr.open_orders("BTC-USD")
    assert len(open_orders) > 0


@pytest.mark.asyncio
async def test_market_making_quotes_symmetric(strategy_setup):
    strategy, order_mgr, book, clock, positions = strategy_setup
    await strategy.on_start()
    ctx = StrategyContext(
        product_id="BTC-USD",
        book=book,
        data_store=strategy._store,
        ts_ns=clock.now_ns(),
    )
    await strategy.on_tick(ctx)
    orders = order_mgr.open_orders("BTC-USD")
    bids = [o for o in orders if o.side.value == "buy"]
    asks = [o for o in orders if o.side.value == "sell"]
    # Should have at least one bid and one ask
    assert len(bids) >= 1
    assert len(asks) >= 1


@pytest.mark.asyncio
async def test_market_making_respects_rate_limit(strategy_setup):
    strategy, order_mgr, book, clock, positions = strategy_setup
    await strategy.on_start()
    ctx = StrategyContext(
        product_id="BTC-USD",
        book=book,
        data_store=strategy._store,
        ts_ns=clock.now_ns(),
    )
    # First tick
    await strategy.on_tick(ctx)
    orders_after_first = len(order_mgr.open_orders("BTC-USD"))

    # Second tick immediately — should NOT post new quotes (rate limited)
    await strategy.on_tick(ctx)
    orders_after_second = len(order_mgr.open_orders("BTC-USD"))

    # Second tick should not add more orders
    assert orders_after_second == orders_after_first


@pytest.mark.asyncio
async def test_strategy_cancels_on_stop(strategy_setup):
    strategy, order_mgr, book, clock, positions = strategy_setup
    await strategy.on_start()
    ctx = StrategyContext(
        product_id="BTC-USD",
        book=book,
        data_store=strategy._store,
        ts_ns=clock.now_ns(),
    )
    await strategy.on_tick(ctx)
    assert len(order_mgr.open_orders("BTC-USD")) > 0
    await strategy.on_stop()
    assert len(order_mgr.open_orders("BTC-USD")) == 0
