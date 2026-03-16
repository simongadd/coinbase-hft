"""Real-time and session PnL accounting."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal

from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.utils.decimal_math import ZERO

logger = logging.getLogger(__name__)


@dataclass
class PnLSnapshot:
    session_realized_pnl: Decimal
    session_unrealized_pnl: Decimal
    session_fees: Decimal
    session_net_pnl: Decimal
    daily_pnl: Decimal
    peak_session_pnl: Decimal
    drawdown_from_peak: Decimal
    drawdown_pct: Decimal    # Fraction of peak (negative = drawdown)


class PnLTracker:
    """Tracks session PnL, peak equity, and drawdown.

    Feeds the circuit breaker with drawdown information.
    """

    def __init__(
        self,
        position_tracker: PositionTracker,
        initial_portfolio_value: Decimal,
    ) -> None:
        self._positions = position_tracker
        self._initial_value = initial_portfolio_value
        self._peak_pnl = ZERO
        self._daily_start_pnl: Decimal = ZERO
        self._day_start_ts = time.time()

    def snapshot(self, current_prices: dict[str, Decimal]) -> PnLSnapshot:
        realized = self._positions.session_realized_pnl
        fees = self._positions.session_fees

        # Compute unrealized from all open positions
        unrealized = ZERO
        for pid, pos in self._positions.all_positions().items():
            price = current_prices.get(pid, ZERO)
            if price > ZERO:
                unrealized += pos.unrealized_pnl(price)

        session_net = realized + unrealized - fees
        self._peak_pnl = max(self._peak_pnl, session_net)
        drawdown = session_net - self._peak_pnl

        drawdown_pct = ZERO
        if self._initial_value > ZERO:
            drawdown_pct = drawdown / self._initial_value

        # Daily PnL resets at midnight UTC
        now = time.time()
        if now - self._day_start_ts > 86400:
            self._daily_start_pnl = session_net
            self._day_start_ts = now

        daily_pnl = session_net - self._daily_start_pnl

        return PnLSnapshot(
            session_realized_pnl=realized,
            session_unrealized_pnl=unrealized,
            session_fees=fees,
            session_net_pnl=session_net,
            daily_pnl=daily_pnl,
            peak_session_pnl=self._peak_pnl,
            drawdown_from_peak=drawdown,
            drawdown_pct=drawdown_pct,
        )
