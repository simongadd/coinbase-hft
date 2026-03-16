"""Main trading engine — orchestrates all components for a single session."""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from decimal import Decimal

from coinbase_hft.config.loader import Settings
from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.market_data_store import MarketDataStore
from coinbase_hft.market_data.websocket_feed import CoinbaseWebSocketFeed
from coinbase_hft.monitoring.alerting import AlertManager
from coinbase_hft.monitoring.health_check import (
    record_tick, set_circuit_breaker, set_ready, set_ws_connected,
)
from coinbase_hft.monitoring.metrics import (
    CIRCUIT_BREAKER_TRIGGERS, ORDERS_REJECTED,
    SESSION_PNL, TICK_PROCESSING_MS, WS_LATENCY_MS, WS_RECONNECTS,
    start_metrics_server,
)
from coinbase_hft.persistence.models import SessionRecord, TradeRecord
from coinbase_hft.persistence.session_recorder import SessionRecorder
from coinbase_hft.persistence.trade_log import TradeLog
from coinbase_hft.risk.circuit_breaker import CircuitBreaker
from coinbase_hft.risk.pnl_tracker import PnLTracker
from coinbase_hft.risk.position_limits import PositionLimits
from coinbase_hft.risk.risk_manager import RiskCheckFailed, RiskManager
from coinbase_hft.strategy.base_strategy import BaseStrategy, StrategyContext
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


class TradingEngine:
    """Orchestrates the full trading session lifecycle.

    Wires together all components and drives the main event loop:
    WebSocket feed → strategy tick → risk check → order submission.
    """

    def __init__(self, settings: Settings, strategy: BaseStrategy) -> None:
        self._settings = settings
        self._strategy = strategy
        self._mode = settings.mode
        self._product_ids = settings.trading_pairs
        self._session_id = time.strftime("%Y-%m-%d_%H-%M-%S") + "_" + str(uuid.uuid4())[:8]
        self._running = False

        # Core infrastructure
        self._clock = Clock()
        self._bus = EventBus()

        # Market data
        self._store = MarketDataStore()
        self._feed: CoinbaseWebSocketFeed | None = None

        # Execution
        initial_balances = {
            k: to_decimal(v)
            for k, v in settings.get("account", "paper_balance", default={}).items()
        }
        if not initial_balances:
            initial_balances = {"USD": Decimal("10000")}

        self._initial_balances = initial_balances
        self._positions = PositionTracker(initial_balances)
        self._fill_model = FillModel(
            slippage_bps=settings.int("paper", "slippage_bps", default=5),
            fill_probability=settings.decimal("paper", "fill_probability", default="0.85"),
            fee_rate=settings.decimal("paper", "fee_rate", default="0.006"),
        )
        self._paper_executor = PaperExecutor(
            fill_model=self._fill_model,
            position_tracker=self._positions,
            event_bus=self._bus,
            clock=self._clock,
        )
        self._order_manager = OrderManager(
            mode=self._mode,
            event_bus=self._bus,
            position_tracker=self._positions,
            clock=self._clock,
            paper_executor=self._paper_executor,
        )

        # Risk
        initial_value = initial_balances.get("USD", Decimal("10000"))
        self._pnl_tracker = PnLTracker(self._positions, initial_value)
        self._circuit_breaker = CircuitBreaker(
            max_drawdown_pct=settings.decimal("risk", "max_drawdown_pct", default="0.02"),
            daily_loss_limit_usd=settings.decimal("risk", "daily_loss_limit_usd", default="200"),
            max_latency_ms=settings.int("risk", "max_latency_ms", default=500),
            max_error_rate=settings.int("risk", "max_error_rate", default=5),
        )
        self._position_limits = PositionLimits(
            position_tracker=self._positions,
            max_position_pct=settings.decimal("risk", "max_position_pct", default="0.25"),
            max_portfolio_exposure_pct=settings.decimal(
                "risk", "max_portfolio_exposure_pct", default="0.80"
            ),
            max_order_size_usd=settings.decimal("risk", "max_order_size_usd", default="500"),
        )
        self._risk_manager = RiskManager(
            position_tracker=self._positions,
            pnl_tracker=self._pnl_tracker,
            circuit_breaker=self._circuit_breaker,
            position_limits=self._position_limits,
            event_bus=self._bus,
            clock=self._clock,
            min_order_interval_ms=settings.int("risk", "min_order_interval_ms", default=50),
        )

        # Persistence
        self._trade_log = TradeLog(settings.str("database", "path", default="data/trades.db"))
        self._recorder = SessionRecorder(self._session_id)

        # Monitoring
        self._alerter = AlertManager(
            slack_webhook=settings.str("monitoring", "alerts", "slack_webhook", default="")
        )

        # Inject engine's own components into the strategy so it uses the
        # correct event bus, order manager and data store regardless of what
        # the caller used when constructing the strategy instance.
        self._strategy._orders = self._order_manager
        self._strategy._store = self._store
        self._strategy._clock = self._clock

        # Register event handlers
        self._bus.subscribe(EventType.ORDER_FILLED, self._on_order_filled)
        self._bus.subscribe(EventType.ORDER_REJECTED, self._on_order_rejected)
        self._bus.subscribe(EventType.CIRCUIT_BREAKER_TRIGGERED, self._on_circuit_breaker)
        self._bus.subscribe(EventType.WS_CONNECTED, self._on_ws_connected)
        self._bus.subscribe(EventType.WS_DISCONNECTED, self._on_ws_disconnected)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        logger.info("=" * 60)
        if self._mode == "paper":
            logger.info("  *** PAPER TRADING MODE — NO REAL ORDERS WILL BE PLACED ***")
        else:
            logger.info("  *** LIVE TRADING MODE — REAL FUNDS AT RISK ***")
        logger.info("  Session ID: %s", self._session_id)
        logger.info("  Strategy:   %s", self._strategy.name)
        logger.info("  Pairs:      %s", ", ".join(self._product_ids))
        logger.info("=" * 60)

        await self._trade_log.open()
        await self._recorder.start()

        start_metrics_server(self._settings.int("monitoring", "prometheus_port", default=9090))

        await self._trade_log.log_session(SessionRecord(
            session_id=self._session_id,
            mode=self._mode,
            strategy=self._strategy.name,
            product_ids=self._product_ids,
            start_ts=self._clock.now_ns(),
            end_ts=None,
            initial_balance={k: str(v) for k, v in self._initial_balances.items()},
            final_balance={},
            realized_pnl="0",
            fees_paid="0",
            trade_count=0,
        ))

        await self._strategy.on_start()
        self._running = True
        set_ready(True)

        # Install kill switch on SIGINT/SIGTERM
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.kill_switch("Signal")))

        self._feed = CoinbaseWebSocketFeed(
            product_ids=self._product_ids,
            event_bus=self._bus,
            data_store=self._store,
            clock=self._clock,
            subscribe_user=(self._mode == "live"),
        )

        # Subscribe to market data events for strategy dispatch
        self._bus.subscribe(EventType.ORDER_BOOK_SNAPSHOT, self._on_book_event)
        self._bus.subscribe(EventType.ORDER_BOOK_DELTA, self._on_book_event)
        self._bus.subscribe(EventType.TICKER, self._on_ticker_event)
        self._bus.subscribe(EventType.TRADE, self._on_trade_event)

        # Start the WebSocket feed in background
        feed_task = asyncio.create_task(self._feed.start())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            await asyncio.gather(feed_task, heartbeat_task)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        logger.info("Engine stopping — cancelling all open orders")

        await self._order_manager.cancel_all_orders()
        if self._feed:
            await self._feed.stop()

        await self._strategy.on_stop()
        await self._recorder.stop()
        await self._trade_log.close()
        set_ready(False)
        logger.info("Engine stopped cleanly")

    async def kill_switch(self, reason: str = "Manual") -> None:
        """Emergency stop: cancel all orders, flatten positions, halt."""
        logger.critical("KILL SWITCH ACTIVATED: %s", reason)
        self._circuit_breaker.trigger_manual(reason)
        await self._order_manager.cancel_all_orders()
        await self._alerter.alert("Kill Switch Activated", reason, "CRITICAL")
        self._running = False
        # Cancel all asyncio tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_book_event(self, event: Event) -> None:
        if not self._running or self._circuit_breaker.is_triggered:
            return
        pid = event.data.get("product_id")
        book = event.data.get("book")
        if not pid or book is None:
            return

        await self._order_manager.on_book_update(pid, book)

        start_ns = self._clock.now_ns()
        ctx = StrategyContext(
            product_id=pid,
            book=book,
            data_store=self._store,
            ts_ns=event.ts_ns,
        )

        current_prices = self._get_current_prices()
        portfolio_value = self._positions.total_portfolio_value(current_prices)

        try:
            await self._risk_manager.tick(current_prices)
        except Exception as exc:
            logger.error("Risk tick error: %s", exc)

        if self._circuit_breaker.is_triggered:
            return

        try:
            await self._strategy.on_tick(ctx)
        except Exception as exc:
            logger.error("Strategy tick error: %s", exc)

        elapsed_ms = (self._clock.now_ns() - start_ns) / 1_000_000
        TICK_PROCESSING_MS.observe(elapsed_ms)
        record_tick()

    async def _on_ticker_event(self, event: Event) -> None:
        pass  # Ticker updates are stored in the MarketDataStore by the feed

    async def _on_trade_event(self, event: Event) -> None:
        """Propagate market trades to paper executor for passive maker fill simulation."""
        if not self._running:
            return
        trade = event.data
        if hasattr(trade, "product_id") and hasattr(trade, "price"):
            await self._order_manager.on_trade_update(trade.product_id, trade.price)

    async def _on_order_filled(self, event: Event) -> None:
        order = event.data.get("order")
        fill = event.data.get("fill")
        if order and fill:
            await self._strategy.on_fill(order, fill)
            record = TradeRecord(
                id=None,
                session_id=self._session_id,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                product_id=order.product_id,
                side=order.side.value,
                order_type=order.order_type.value,
                size=order.size,
                filled_size=fill.filled_size,
                avg_fill_price=fill.avg_fill_price,
                limit_price=order.limit_price,
                fee=fill.fee,
                mode=self._mode,
                status=order.status.name,
                created_ts_ns=order.created_ts_ns,
                filled_ts_ns=order.filled_ts_ns,
            )
            await self._trade_log.log_trade(record)

            pnl = self._risk_manager.pnl_snapshot(self._get_current_prices())
            SESSION_PNL.labels(mode=self._mode).set(float(pnl.session_net_pnl))

    async def _on_order_rejected(self, event: Event) -> None:
        self._risk_manager.record_order_error()
        ORDERS_REJECTED.labels(
            product_id=event.data.get("product_id", "unknown"),
            reason=event.data.get("reason", "unknown"),
        ).inc()

    async def _on_circuit_breaker(self, event: Event) -> None:
        detail = event.data.get("detail", "")
        reason_name = "UNKNOWN"
        cb_event = event.data.get("event")
        if cb_event:
            reason_name = cb_event.reason.name
        set_circuit_breaker(True)
        CIRCUIT_BREAKER_TRIGGERS.labels(reason=reason_name).inc()
        await self._alerter.circuit_breaker_alert(reason_name, detail)
        logger.critical("CIRCUIT BREAKER: %s — %s", reason_name, detail)
        await self.stop()

    async def _on_ws_connected(self, event: Event) -> None:
        set_ws_connected(True)

    async def _on_ws_disconnected(self, event: Event) -> None:
        set_ws_connected(False)
        WS_RECONNECTS.inc()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_current_prices(self) -> dict[str, Decimal]:
        prices: dict[str, Decimal] = {}
        for pid in self._product_ids:
            ticker = self._store.ticker(pid)
            if ticker:
                prices[pid] = ticker.price
        return prices

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(10)
            if self._feed:
                last_msg = self._feed.last_message_ts
                if last_msg > 0:
                    latency_ms = (time.monotonic() - last_msg) * 1000
                    WS_LATENCY_MS.set(latency_ms)
            pnl = self._risk_manager.pnl_snapshot(self._get_current_prices())
            logger.info(
                "Heartbeat | mode=%s pnl=%.4f drawdown=%.2f%% orders_open=%d",
                self._mode,
                pnl.session_net_pnl,
                float(pnl.drawdown_pct) * 100,
                sum(len(self._order_manager.open_orders(pid)) for pid in self._product_ids),
            )
