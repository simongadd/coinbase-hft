"""Real-time position and exposure tracking."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


@dataclass
class Position:
    product_id: str
    size: Decimal = ZERO           # Positive = long, negative = short
    avg_entry_price: Decimal = ZERO
    realized_pnl: Decimal = ZERO
    total_fees_paid: Decimal = ZERO

    def unrealized_pnl(self, current_price: Decimal) -> Decimal:
        if self.size == ZERO or self.avg_entry_price == ZERO:
            return ZERO
        if self.size > ZERO:
            return (current_price - self.avg_entry_price) * self.size
        else:
            return (self.avg_entry_price - current_price) * abs(self.size)

    def notional_value(self, current_price: Decimal) -> Decimal:
        return abs(self.size) * current_price

    @property
    def is_flat(self) -> bool:
        return self.size == ZERO

    @property
    def is_long(self) -> bool:
        return self.size > ZERO

    @property
    def is_short(self) -> bool:
        return self.size < ZERO


class PositionTracker:
    """Tracks all open positions and cash balance.

    Updated on every fill event. Provides portfolio-level exposure queries
    needed by the risk manager.
    """

    def __init__(self, initial_balances: dict[str, Decimal]) -> None:
        # e.g. {"USD": Decimal("10000"), "BTC": Decimal("0")}
        self._cash = {k: to_decimal(v) for k, v in initial_balances.items()}
        self._positions: dict[str, Position] = {}
        self._session_realized_pnl: Decimal = ZERO
        self._session_fees: Decimal = ZERO

    # ------------------------------------------------------------------
    # Fill processing
    # ------------------------------------------------------------------

    def on_fill(
        self,
        product_id: str,
        side: str,          # "buy" | "sell"
        filled_size: Decimal,
        avg_fill_price: Decimal,
        fee: Decimal,
    ) -> None:
        """Update position and cash on a fill."""
        base_currency, quote_currency = product_id.split("-")

        pos = self._positions.setdefault(product_id, Position(product_id=product_id))

        if side == "buy":
            notional = avg_fill_price * filled_size
            # Update average entry price (FIFO)
            if pos.size >= ZERO:
                # Adding to or opening long
                total_cost = pos.avg_entry_price * pos.size + notional
                pos.size += filled_size
                pos.avg_entry_price = total_cost / pos.size if pos.size != ZERO else ZERO
            else:
                # Closing short
                close_size = min(filled_size, abs(pos.size))
                realized = (pos.avg_entry_price - avg_fill_price) * close_size
                pos.realized_pnl += realized
                self._session_realized_pnl += realized
                pos.size += filled_size
                if pos.size > ZERO:
                    pos.avg_entry_price = avg_fill_price
                elif pos.size == ZERO:
                    pos.avg_entry_price = ZERO

            # Deduct cash for buy
            self._cash[quote_currency] = self._cash.get(quote_currency, ZERO) - notional - fee
            self._cash[base_currency] = self._cash.get(base_currency, ZERO) + filled_size

        else:  # sell
            notional = avg_fill_price * filled_size
            if pos.size > ZERO:
                # Closing long
                close_size = min(filled_size, pos.size)
                realized = (avg_fill_price - pos.avg_entry_price) * close_size
                pos.realized_pnl += realized
                self._session_realized_pnl += realized
                pos.size -= filled_size
                if pos.size < ZERO:
                    pos.avg_entry_price = avg_fill_price
                elif pos.size == ZERO:
                    pos.avg_entry_price = ZERO
            else:
                # Opening or adding to short
                total_cost = pos.avg_entry_price * abs(pos.size) + notional
                pos.size -= filled_size
                pos.avg_entry_price = total_cost / abs(pos.size) if pos.size != ZERO else ZERO

            # Add cash for sell
            self._cash[quote_currency] = self._cash.get(quote_currency, ZERO) + notional - fee
            self._cash[base_currency] = self._cash.get(base_currency, ZERO) - filled_size

        pos.total_fees_paid += fee
        self._session_fees += fee

        logger.debug(
            "Position update %s side=%s size=%s price=%s → pos=%s entry=%s rpnl=%s",
            product_id, side, filled_size, avg_fill_price,
            pos.size, pos.avg_entry_price, pos.realized_pnl,
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def position(self, product_id: str) -> Position:
        return self._positions.get(product_id, Position(product_id=product_id))

    def all_positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def cash(self, currency: str) -> Decimal:
        return self._cash.get(currency, ZERO)

    def total_portfolio_value(self, prices: dict[str, Decimal]) -> Decimal:
        """Approximate portfolio value in USD using provided mark prices."""
        total = self._cash.get("USD", ZERO)
        for product_id, pos in self._positions.items():
            if pos.is_flat:
                continue
            base = product_id.split("-")[0]
            price = prices.get(product_id, prices.get(base, ZERO))
            total += abs(pos.size) * price
        return total

    def total_exposure_usd(self, prices: dict[str, Decimal]) -> Decimal:
        total = ZERO
        for product_id, pos in self._positions.items():
            if pos.is_flat:
                continue
            price = prices.get(product_id, ZERO)
            total += pos.notional_value(price)
        return total

    @property
    def session_realized_pnl(self) -> Decimal:
        return self._session_realized_pnl

    @property
    def session_fees(self) -> Decimal:
        return self._session_fees

    def session_net_pnl(self) -> Decimal:
        return self._session_realized_pnl - self._session_fees

    def snapshot(self) -> dict:
        return {
            "cash": {k: str(v) for k, v in self._cash.items()},
            "positions": {
                pid: {
                    "size": str(p.size),
                    "avg_entry": str(p.avg_entry_price),
                    "realized_pnl": str(p.realized_pnl),
                    "fees_paid": str(p.total_fees_paid),
                }
                for pid, p in self._positions.items()
            },
            "session_realized_pnl": str(self._session_realized_pnl),
            "session_fees": str(self._session_fees),
        }
