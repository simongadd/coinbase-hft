"""Tests for the SlowMarketMakerStrategy."""

from __future__ import annotations

import math
import time
from decimal import Decimal
from unittest.mock import patch, AsyncMock

import pytest

from coinbase_hft.core.clock import SimulatedClock
from coinbase_hft.core.event_bus import EventBus
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor, OrderStatus
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.market_data_store import MarketDataStore, TickerSnapshot, TradeEvent
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.strategy.base_strategy import StrategyContext
from coinbase_hft.strategy.examples.slow_market_maker import SlowMarketMakerStrategy
from coinbase_hft.utils.product_info import ProductMeta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PRODUCT_ID = "BTC-GBP"

_FAKE_META = {
    PRODUCT_ID: ProductMeta(
        product_id=PRODUCT_ID,
        quote_increment=Decimal("0.01"),
        base_increment=Decimal("0.00000001"),
        base_min_size=Decimal("0.0001"),
        base_max_size=Decimal("1000"),
    ),
    "ETH-GBP": ProductMeta(
        product_id="ETH-GBP",
        quote_increment=Decimal("0.01"),
        base_increment=Decimal("0.00000001"),
        base_min_size=Decimal("0.001"),
        base_max_size=Decimal("10000"),
    ),
}


def _make_book(pid: str, bid: str, ask: str, bid_qty: str = "10.0", ask_qty: str = "10.0") -> OrderBook:
    book = OrderBook(pid)
    book.apply_snapshot(bids=[[bid, bid_qty]], asks=[[ask, ask_qty]])
    return book


def _make_ticker(store: MarketDataStore, clock: SimulatedClock, pid: str, bid: str, ask: str) -> None:
    store.update_ticker(TickerSnapshot(
        product_id=pid,
        price=Decimal(bid),
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        volume_24h=Decimal("1000"),
        ts_ns=clock.now_ns(),
    ))


def _smm_strategy(
    product_ids: list[str],
    order_manager: OrderManager,
    store: MarketDataStore,
    clock: SimulatedClock,
    config: dict | None = None,
) -> SlowMarketMakerStrategy:
    default_config = {
        "global_defaults": {
            "spread_bps": 30,
            "gamma": "0.1",
            "order_size": "0.01",
            "num_rungs": 3,
            "rung_weights": ["0.5", "0.3", "0.2"],
            "rung_spacing_bps": 10,
            "quote_refresh_interval_s": 0.0,   # always requote in tests
            "drift_threshold_bps": 0,           # always requote
            "post_only": False,
            "vol_window_s": 300,
            "vol_halt_pct": "0.05",
            "halt_cooldown_s": 1,
            "min_viable_profit_bps": 0,
        },
        "pair_overrides": {},
        "fee_rate_bps": 6,
    }
    if config:
        default_config.update(config)
    return SlowMarketMakerStrategy(
        product_ids=product_ids,
        order_manager=order_manager,
        data_store=store,
        clock=clock,
        config=default_config,
    )


async def _start_with_fake_meta(strategy: SlowMarketMakerStrategy) -> None:
    with patch(
        "coinbase_hft.strategy.examples.slow_market_maker.fetch_product_metas",
        new=AsyncMock(return_value=_FAKE_META),
    ):
        await strategy.on_start()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def clock():
    return SimulatedClock(start_ns=1_000_000_000_000)


@pytest.fixture
def event_bus():
    return EventBus()


@pytest.fixture
def position_tracker():
    return PositionTracker({
        "USD": Decimal("100000"),
        "GBP": Decimal("100000"),
        "EUR": Decimal("100000"),
        "BTC": Decimal("0"),
        "ETH": Decimal("0"),
    })


@pytest.fixture
def fill_model():
    return FillModel(
        slippage_bps=0,
        fill_probability=Decimal("1.0"),
        fee_rate=Decimal("0.0006"),
        queue_position_factor=Decimal("0.0"),
    )


@pytest.fixture
def paper_executor(fill_model, position_tracker, event_bus, clock):
    return PaperExecutor(fill_model, position_tracker, event_bus, clock)


@pytest.fixture
def order_manager(event_bus, position_tracker, clock, paper_executor):
    return OrderManager(
        mode="paper",
        event_bus=event_bus,
        position_tracker=position_tracker,
        clock=clock,
        paper_executor=paper_executor,
    )


@pytest.fixture
def store():
    return MarketDataStore()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_posts_ladder_quotes(order_manager, store, clock):
    """Strategy should post num_rungs bids and num_rungs asks."""
    strategy = _smm_strategy([PRODUCT_ID], order_manager, store, clock)
    await _start_with_fake_meta(strategy)

    book = _make_book(PRODUCT_ID, "50000.00", "50010.00")
    _make_ticker(store, clock, PRODUCT_ID, "50000.00", "50010.00")

    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx)

    open_orders = order_manager.open_orders(PRODUCT_ID)
    bids = [o for o in open_orders if o.side.value == "buy"]
    asks = [o for o in open_orders if o.side.value == "sell"]

    assert len(bids) == 3, f"Expected 3 bid rungs, got {len(bids)}"
    assert len(asks) == 3, f"Expected 3 ask rungs, got {len(asks)}"


@pytest.mark.asyncio
async def test_inventory_skew_moves_reservation(order_manager, store, clock, position_tracker):
    """Positive inventory should shift reservation price below mid compared to flat inventory."""
    now_ns = clock.now_ns()

    # Add trade history so sigma > 0 (A-S skew requires non-zero vol)
    prices = [50000, 50010, 49990, 50020, 49980, 50005, 50015, 49995]
    for i, p in enumerate(prices):
        store.add_trade(TradeEvent(
            product_id=PRODUCT_ID,
            price=Decimal(str(p)),
            size=Decimal("0.01"),
            side="buy",
            trade_id=f"t{i}",
            ts_ns=now_ns - (len(prices) - i) * 10_000_000_000,  # spread over time
        ))

    strategy_long = _smm_strategy([PRODUCT_ID], order_manager, store, clock)
    await _start_with_fake_meta(strategy_long)

    # Set a long BTC-GBP position (q > 0 → skew pushes reservation down)
    position_tracker.on_fill(
        product_id=PRODUCT_ID,
        side="buy",
        filled_size=Decimal("2.0"),
        avg_fill_price=Decimal("50000"),
        fee=Decimal("0"),
    )

    book = _make_book(PRODUCT_ID, "50000.00", "50010.00")
    _make_ticker(store, clock, PRODUCT_ID, "50000.00", "50010.00")
    mid = Decimal("50005")

    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=now_ns)
    await strategy_long.on_tick(ctx)

    open_orders = order_manager.open_orders(PRODUCT_ID)
    bids = sorted([o for o in open_orders if o.side.value == "buy"], key=lambda o: -o.limit_price)
    asks = sorted([o for o in open_orders if o.side.value == "sell"], key=lambda o: o.limit_price)

    assert len(bids) > 0 and len(asks) > 0
    # Implied reservation (midpoint of best bid+ask) should be < mid when long with non-zero vol
    best_bid_price = bids[0].limit_price
    best_ask_price = asks[0].limit_price
    implied_reservation = (best_bid_price + best_ask_price) / Decimal("2")
    assert implied_reservation < mid, (
        f"Implied reservation {implied_reservation} should be < mid {mid} when long with vol"
    )


@pytest.mark.asyncio
async def test_min_viable_spread_floor(order_manager, store, clock):
    """High latency should raise spread floor above the configured spread_bps."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 1,          # very tight
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 1,
                "rung_weights": ["1.0"],
                "rung_spacing_bps": 0,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.99",
                "halt_cooldown_s": 1,
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    # Inject high latency samples — p95 = 200ms
    for _ in range(100):
        store.record_latency_sample(200.0)

    book = _make_book(PRODUCT_ID, "50000.00", "50010.00")
    _make_ticker(store, clock, PRODUCT_ID, "50000.00", "50010.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx)

    open_orders = order_manager.open_orders(PRODUCT_ID)
    bids = [o for o in open_orders if o.side.value == "buy"]
    asks = [o for o in open_orders if o.side.value == "sell"]
    assert len(bids) == 1 and len(asks) == 1

    mid = Decimal("50005")
    half_spread_actual = (asks[0].limit_price - mid) / mid * Decimal("10000")
    # spread_bps=1 → half_spread=0.5bps; with 200ms latency, floor >> 1bps
    # latency_risk_bps = 200/1000 * 100 * 2 = 40, fee_bps=12, min_profit=0
    # min_viable = 52, half = 26bps
    assert half_spread_actual > Decimal("1"), (
        f"Spread {half_spread_actual:.2f}bps should be > 1bps due to latency floor"
    )


@pytest.mark.asyncio
async def test_drift_suppresses_requote(order_manager, store, clock):
    """Tick within drift threshold should not trigger cancel/resubmit."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 3,
                "rung_weights": ["0.5", "0.3", "0.2"],
                "rung_spacing_bps": 10,
                "quote_refresh_interval_s": 60.0,   # long refresh interval
                "drift_threshold_bps": 100,           # 100bps drift required
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.99",
                "halt_cooldown_s": 1,
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    book = _make_book(PRODUCT_ID, "50000.00", "50010.00")
    _make_ticker(store, clock, PRODUCT_ID, "50000.00", "50010.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=clock.now_ns())

    # First tick — should post
    await strategy.on_tick(ctx)
    orders_after_first = len(order_manager.open_orders(PRODUCT_ID))
    assert orders_after_first > 0

    # Second tick — same price, within refresh interval and drift threshold → no new orders
    await strategy.on_tick(ctx)
    orders_after_second = len(order_manager.open_orders(PRODUCT_ID))
    assert orders_after_second == orders_after_first, (
        "Should not resubmit within refresh interval if drift below threshold"
    )


@pytest.mark.asyncio
async def test_vol_halt_triggers(order_manager, store, clock):
    """A large price move in the vol window should trigger a halt."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 1,
                "rung_weights": ["1.0"],
                "rung_spacing_bps": 0,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.02",   # 2% halt threshold
                "halt_cooldown_s": 60,
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    now_ns = clock.now_ns()
    # Inject trades: old price 50000, new price 52000 (+4% > 2% threshold)
    store.add_trade(TradeEvent(
        product_id=PRODUCT_ID,
        price=Decimal("50000"),
        size=Decimal("0.01"),
        side="buy",
        trade_id="t1",
        ts_ns=now_ns - 60_000_000_000,  # 60s ago
    ))
    store.add_trade(TradeEvent(
        product_id=PRODUCT_ID,
        price=Decimal("52100"),  # +4.2%
        size=Decimal("0.01"),
        side="buy",
        trade_id="t2",
        ts_ns=now_ns,
    ))

    book = _make_book(PRODUCT_ID, "52095.00", "52105.00")
    _make_ticker(store, clock, PRODUCT_ID, "52095.00", "52105.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=now_ns)
    await strategy.on_tick(ctx)

    state = strategy._pair_states[PRODUCT_ID]
    assert state.halted, "Strategy should be halted after large price move"
    assert len(order_manager.open_orders(PRODUCT_ID)) == 0, "All orders should be cancelled on halt"


@pytest.mark.asyncio
async def test_halt_reentry_all_4_conditions(order_manager, store, clock):
    """Strategy should not resume quoting until price returns to within 0.5% of halt ref."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 1,
                "rung_weights": ["1.0"],
                "rung_spacing_bps": 0,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.02",
                "halt_cooldown_s": 0,   # immediate cooldown expiry
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    # Manually put the strategy in halted state
    state = strategy._pair_states[PRODUCT_ID]
    state.halted = True
    state.halt_until = time.monotonic() - 1.0  # cooldown expired
    state.halt_ref_price = Decimal("50000")
    state.halt_extensions = 0

    # Price still far away (>0.5% from halt ref) → no reentry
    book_far = _make_book(PRODUCT_ID, "49000.00", "49010.00")  # -2% from ref
    _make_ticker(store, clock, PRODUCT_ID, "49000.00", "49010.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book_far, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx)

    # Should still be halted
    assert state.halted, "Should remain halted when price hasn't returned"
    assert len(order_manager.open_orders(PRODUCT_ID)) == 0

    # Price returned close to halt ref (within 0.5%)
    state.halt_until = time.monotonic() - 1.0  # reset cooldown
    state.halt_extensions = 0
    book_near = _make_book(PRODUCT_ID, "50001.00", "50003.00")  # ~0.002% from ref
    _make_ticker(store, clock, PRODUCT_ID, "50001.00", "50003.00")
    ctx2 = StrategyContext(product_id=PRODUCT_ID, book=book_near, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx2)

    # Now should be unhalted and quoting
    assert not state.halted, "Should be unhalted when price returned"
    assert len(order_manager.open_orders(PRODUCT_ID)) > 0, "Should be quoting after halt lifted"


@pytest.mark.asyncio
async def test_cooldown_extension(order_manager, store, clock):
    """Failed re-entry should extend halt cooldown (up to 5×)."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 1,
                "rung_weights": ["1.0"],
                "rung_spacing_bps": 0,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.02",
                "halt_cooldown_s": 1,
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    state = strategy._pair_states[PRODUCT_ID]
    state.halted = True
    state.halt_until = time.monotonic() - 1.0
    state.halt_ref_price = Decimal("50000")
    state.halt_extensions = 0

    # Price is far — should fail re-entry and extend cooldown
    initial_halt_until = state.halt_until
    book_far = _make_book(PRODUCT_ID, "48000.00", "48010.00")  # -4% from ref
    _make_ticker(store, clock, PRODUCT_ID, "48000.00", "48010.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book_far, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx)

    assert state.halt_extensions >= 1, "Halt should have been extended"
    assert state.halt_until > time.monotonic(), "Cooldown should be in the future after extension"


@pytest.mark.asyncio
async def test_post_only_rejected_not_filled(order_manager, store, clock):
    """A post-only order that would cross the spread should be rejected, not filled."""
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",
                "num_rungs": 1,
                "rung_weights": ["1.0"],
                "rung_spacing_bps": 0,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": True,         # post-only enabled
                "vol_window_s": 300,
                "vol_halt_pct": "0.99",
                "halt_cooldown_s": 1,
                "min_viable_profit_bps": 0,
            }
        },
    )
    await _start_with_fake_meta(strategy)

    # Very tight spread — bid at 50000, ask at 50001
    # With 30bps half-spread, our bids should be ~50000 * (1 - 0.003) ≈ 49850 — fine
    # But let's test directly via paper_executor with a crossing price
    book = _make_book(PRODUCT_ID, "50000.00", "50001.00", "100.0", "100.0")

    # Submit a buy limit that crosses the ask — should be rejected with post_only=True
    order = await order_manager.submit_order(
        product_id=PRODUCT_ID,
        side="buy",
        order_type="limit",
        size=Decimal("0.01"),
        limit_price=Decimal("50002.00"),  # above best ask → crossing
        book=book,
        post_only=True,
    )
    assert order is not None
    assert order.status == OrderStatus.REJECTED, (
        f"Post-only buy at 50002 when ask=50001 should be REJECTED, got {order.status}"
    )


@pytest.mark.asyncio
async def test_subminimum_rung_merging(order_manager, store, clock):
    """Rung sizes below base_min_size should be merged up to the minimum."""
    # Use a very large base_min_size so tiny rung_weights get clamped
    fake_meta_large_min = {
        PRODUCT_ID: ProductMeta(
            product_id=PRODUCT_ID,
            quote_increment=Decimal("0.01"),
            base_increment=Decimal("0.00000001"),
            base_min_size=Decimal("0.005"),   # larger minimum
            base_max_size=Decimal("1000"),
        )
    }
    strategy = _smm_strategy(
        [PRODUCT_ID], order_manager, store, clock,
        config={
            "global_defaults": {
                "spread_bps": 30,
                "gamma": "0.0",
                "order_size": "0.01",   # total size
                "num_rungs": 3,
                "rung_weights": ["0.5", "0.3", "0.2"],  # 0.005, 0.003, 0.002
                "rung_spacing_bps": 10,
                "quote_refresh_interval_s": 0.0,
                "drift_threshold_bps": 0,
                "post_only": False,
                "vol_window_s": 300,
                "vol_halt_pct": "0.99",
                "halt_cooldown_s": 1,
                "min_viable_profit_bps": 0,
            }
        },
    )

    with patch(
        "coinbase_hft.strategy.examples.slow_market_maker.fetch_product_metas",
        new=AsyncMock(return_value=fake_meta_large_min),
    ):
        await strategy.on_start()

    book = _make_book(PRODUCT_ID, "50000.00", "50010.00")
    _make_ticker(store, clock, PRODUCT_ID, "50000.00", "50010.00")
    ctx = StrategyContext(product_id=PRODUCT_ID, book=book, data_store=store, ts_ns=clock.now_ns())
    await strategy.on_tick(ctx)

    # All submitted orders should have size >= base_min_size
    open_orders = order_manager.open_orders(PRODUCT_ID)
    min_size = Decimal("0.005")
    for o in open_orders:
        assert o.size >= min_size, (
            f"Order size {o.size} should be >= base_min_size {min_size}"
        )
