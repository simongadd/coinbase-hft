"""Pre-trade and post-trade risk manager — all orders must pass through here."""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.risk.circuit_breaker import CircuitBreaker
from coinbase_hft.risk.pnl_tracker import PnLSnapshot, PnLTracker
from coinbase_hft.risk.position_limits import PositionLimits
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


class RiskCheckFailed(Exception):
    pass


class RiskManager:
    """Enforces all pre-trade and post-trade risk controls.

    Every order request from a strategy must call `check_order()` first.
    Continuous monitoring runs via `tick()` which should be called regularly.
    """

    def __init__(
        self,
        position_tracker: PositionTracker,
        pnl_tracker: PnLTracker,
        circuit_breaker: CircuitBreaker,
        position_limits: PositionLimits,
        event_bus: EventBus,
        clock: Clock,
        min_order_interval_ms: int = 50,
        max_spread_bps: Decimal | None = None,
    ) -> None:
        self._positions = position_tracker
        self._pnl = pnl_tracker
        self._cb = circuit_breaker
        self._limits = position_limits
        self._bus = event_bus
        self._clock = clock
        self.min_order_interval_ms = min_order_interval_ms
        self.max_spread_bps = max_spread_bps
        self._last_order_ts: dict[str, int] = {}  # product_id → ts_ns

    # ------------------------------------------------------------------
    # Pre-trade checks
    # ------------------------------------------------------------------

    async def check_order(
        self,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal,
        price: Decimal | None,
        current_prices: dict[str, Decimal],
        portfolio_value_usd: Decimal,
        book_spread_bps: Decimal | None = None,
    ) -> None:
        """Raise RiskCheckFailed if the order should be blocked.

        This is the single gate all orders pass through before submission.
        """
        # 1. Circuit breaker — hard stop
        if self._cb.is_triggered:
            event = self._cb.trigger_event
            raise RiskCheckFailed(
                f"Circuit breaker active [{event.reason.name if event else 'UNKNOWN'}] — "
                "trading halted"
            )

        # 2. Min order interval (throttle rapid-fire orders per pair)
        now_ns = self._clock.now_ns()
        last_ns = self._last_order_ts.get(product_id, 0)
        elapsed_ms = (now_ns - last_ns) / 1_000_000
        if elapsed_ms < self.min_order_interval_ms:
            raise RiskCheckFailed(
                f"Order throttled: {elapsed_ms:.1f}ms since last order "
                f"(min {self.min_order_interval_ms}ms)"
            )

        # 3. Spread sanity check
        if self.max_spread_bps is not None and book_spread_bps is not None:
            if book_spread_bps > self.max_spread_bps:
                raise RiskCheckFailed(
                    f"Spread too wide: {book_spread_bps:.1f} bps > {self.max_spread_bps:.1f} bps"
                )

        # 4. Order size limit
        order_price = price or current_prices.get(product_id, ZERO)
        size_usd = size * order_price
        ok, reason = self._limits.check_order_size(size_usd)
        if not ok:
            raise RiskCheckFailed(reason)

        # 5. Per-pair position limit
        ok, reason = self._limits.check_position_limit(
            product_id, side, size_usd, portfolio_value_usd, current_prices
        )
        if not ok:
            raise RiskCheckFailed(reason)

        # 6. Portfolio exposure limit
        ok, reason = self._limits.check_portfolio_exposure(
            size_usd, portfolio_value_usd, current_prices
        )
        if not ok:
            raise RiskCheckFailed(reason)

        # Record this order for interval throttling
        self._last_order_ts[product_id] = now_ns
        logger.debug("Risk check PASSED: %s %s %s @ %s", side, size, product_id, price or "MARKET")

    # ------------------------------------------------------------------
    # Continuous monitoring (call on each tick)
    # ------------------------------------------------------------------

    async def tick(
        self,
        current_prices: dict[str, Decimal],
        ws_latency_ms: float = 0.0,
    ) -> None:
        """Check continuous risk conditions. Triggers circuit breaker if needed."""
        pnl = self._pnl.snapshot(current_prices)

        if self._cb.check_drawdown(pnl.drawdown_pct):
            await self._emit_breach(
                f"Max drawdown {float(pnl.drawdown_pct)*100:.2f}% breached"
            )
            return

        if self._cb.check_daily_loss(pnl.daily_pnl):
            await self._emit_breach(
                f"Daily loss ${pnl.daily_pnl:.2f} limit breached"
            )
            return

        if ws_latency_ms > 0 and self._cb.check_latency(ws_latency_ms):
            await self._emit_breach(
                f"WebSocket latency {ws_latency_ms:.0f}ms too high"
            )
            return

    def record_order_error(self) -> None:
        """Call when an order is rejected/errored by the exchange."""
        if self._cb.record_error():
            logger.critical("CIRCUIT BREAKER: error rate exceeded")

    async def _emit_breach(self, detail: str) -> None:
        await self._bus.publish_sync(Event(
            type=EventType.CIRCUIT_BREAKER_TRIGGERED,
            data={"detail": detail, "event": self._cb.trigger_event},
            source="risk_manager",
            ts_ns=self._clock.now_ns(),
        ))
        logger.critical("RISK BREACH: %s", detail)

    def pnl_snapshot(self, current_prices: dict[str, Decimal]) -> PnLSnapshot:
        return self._pnl.snapshot(current_prices)
