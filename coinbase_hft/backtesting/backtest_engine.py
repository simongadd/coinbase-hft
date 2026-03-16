"""Historical replay backtesting engine."""

from __future__ import annotations

import logging
from decimal import Decimal

from coinbase_hft.backtesting.data_loader import HistoricalCandle
from coinbase_hft.backtesting.performance import PerformanceReport, compute_performance
from coinbase_hft.core.clock import SimulatedClock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.market_data_store import MarketDataStore, TickerSnapshot
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.strategy.base_strategy import BaseStrategy, StrategyContext
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Replay historical candles through a strategy and collect performance metrics.

    Uses simulated clock, paper executor, and in-memory market data store.
    No live connections are made during backtesting.
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        product_ids: list[str],
        initial_balance_usd: Decimal = Decimal("10000"),
        slippage_bps: int = 5,
        fee_rate: Decimal = Decimal("0.006"),
    ) -> None:
        self.strategy = strategy
        self.product_ids = product_ids
        self._initial_balance = initial_balance_usd
        self._clock = SimulatedClock()
        self._bus = EventBus()
        initial_balances = {"USD": initial_balance_usd}
        self._positions = PositionTracker(initial_balances)
        self._fill_model = FillModel(
            slippage_bps=slippage_bps,
            fee_rate=fee_rate,
        )
        self._paper_executor = PaperExecutor(
            fill_model=self._fill_model,
            position_tracker=self._positions,
            event_bus=self._bus,
            clock=self._clock,
        )
        self._store = MarketDataStore()
        self._books: dict[str, OrderBook] = {pid: OrderBook(pid) for pid in product_ids}
        self._order_manager = OrderManager(
            mode="paper",
            event_bus=self._bus,
            position_tracker=self._positions,
            clock=self._clock,
            paper_executor=self._paper_executor,
        )
        # Inject dependencies into strategy
        strategy._orders = self._order_manager
        strategy._store = self._store
        strategy._clock = self._clock

        self._trade_pnls: list[Decimal] = []
        self._bus.subscribe(EventType.ORDER_FILLED, self._on_fill)

    async def _on_fill(self, event: Event) -> None:
        order = event.data.get("order")
        if order:
            pnl = self._positions.session_realized_pnl
            self._trade_pnls.append(pnl - (sum(self._trade_pnls) if self._trade_pnls else ZERO))

    async def run(self, candles: list[HistoricalCandle]) -> PerformanceReport:
        """Replay candles and return performance report."""
        await self.strategy.on_start()
        logger.info(
            "Backtest starting: %d candles, strategy=%s",
            len(candles), self.strategy.name,
        )

        for candle in candles:
            self._clock.set_ns(candle.open_time * 1_000_000_000)

            # Build a synthetic order book from OHLCV data
            pid = candle.product_id
            if pid not in self._books:
                self._books[pid] = OrderBook(pid)
            book = self._books[pid]

            # Simulate a simple book using close price ± half spread
            spread = candle.close * Decimal("0.001")  # 10 bps synthetic spread
            bid = candle.close - spread / 2
            ask = candle.close + spread / 2
            book.apply_snapshot(
                bids=[[str(bid), str(candle.volume / 2)]],
                asks=[[str(ask), str(candle.volume / 2)]],
            )

            # Update store
            snap = TickerSnapshot(
                product_id=pid,
                price=candle.close,
                best_bid=bid,
                best_ask=ask,
                volume_24h=candle.volume,
                ts_ns=self._clock.now_ns(),
            )
            self._store.update_ticker(snap)
            self._store.add_candle(candle.to_candle())

            # Feed strategy tick
            ctx = StrategyContext(
                product_id=pid,
                book=book,
                data_store=self._store,
                ts_ns=self._clock.now_ns(),
            )
            await self.strategy.on_tick(ctx)

            # Try to fill any resting limit orders
            await self._order_manager.on_book_update(pid, book)

        await self.strategy.on_stop()

        report = compute_performance(self._trade_pnls, self._initial_balance)
        logger.info("Backtest complete:\n%s", report.display())
        return report
