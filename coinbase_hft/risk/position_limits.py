"""Per-pair and portfolio-level position limit checks."""

from __future__ import annotations

import logging
from decimal import Decimal

from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.utils.decimal_math import ZERO

logger = logging.getLogger(__name__)


class PositionLimits:
    """Pre-trade position limit enforcement.

    All limits are read from settings and enforced before each order.
    """

    def __init__(
        self,
        position_tracker: PositionTracker,
        max_position_pct: Decimal,
        max_portfolio_exposure_pct: Decimal,
        max_order_size_usd: Decimal,
    ) -> None:
        self._positions = position_tracker
        self.max_position_pct = max_position_pct
        self.max_portfolio_exposure_pct = max_portfolio_exposure_pct
        self.max_order_size_usd = max_order_size_usd

    def check_order_size(self, size_usd: Decimal) -> tuple[bool, str]:
        """Reject if order notional exceeds absolute size limit."""
        if size_usd > self.max_order_size_usd:
            return False, (
                f"Order size ${size_usd:.2f} exceeds max ${self.max_order_size_usd:.2f}"
            )
        return True, ""

    def check_position_limit(
        self,
        product_id: str,
        side: str,
        size_usd: Decimal,
        portfolio_value_usd: Decimal,
        current_prices: dict[str, Decimal],
    ) -> tuple[bool, str]:
        """Reject if resulting position exceeds max_position_pct of portfolio."""
        if portfolio_value_usd <= ZERO:
            return True, ""

        pos = self._positions.position(product_id)
        current_exposure = abs(pos.size) * current_prices.get(product_id, ZERO)

        if side == "buy":
            new_exposure = current_exposure + size_usd
        else:
            new_exposure = max(ZERO, current_exposure - size_usd)

        max_allowed = portfolio_value_usd * self.max_position_pct
        if new_exposure > max_allowed:
            return False, (
                f"Position limit breach: ${new_exposure:.2f} > "
                f"{float(self.max_position_pct)*100:.0f}% of ${portfolio_value_usd:.2f}"
            )
        return True, ""

    def check_portfolio_exposure(
        self,
        size_usd: Decimal,
        portfolio_value_usd: Decimal,
        current_prices: dict[str, Decimal],
    ) -> tuple[bool, str]:
        """Reject if total portfolio exposure would exceed threshold."""
        if portfolio_value_usd <= ZERO:
            return True, ""
        current_exposure = self._positions.total_exposure_usd(current_prices)
        new_exposure = current_exposure + size_usd
        max_allowed = portfolio_value_usd * self.max_portfolio_exposure_pct
        if new_exposure > max_allowed:
            return False, (
                f"Portfolio exposure breach: ${new_exposure:.2f} > "
                f"{float(self.max_portfolio_exposure_pct)*100:.0f}% of ${portfolio_value_usd:.2f}"
            )
        return True, ""
