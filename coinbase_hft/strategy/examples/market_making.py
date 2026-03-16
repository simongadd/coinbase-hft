"""Market-making strategy: spread capture with inventory management.

Posts symmetric bid/ask quotes around the mid-price, skewing the spread
when inventory grows to encourage reversion to flat.
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any

from coinbase_hft.strategy.base_strategy import BaseStrategy, StrategyContext
from coinbase_hft.strategy.strategy_registry import register
from coinbase_hft.strategy.signals.microstructure import order_flow_imbalance
from coinbase_hft.utils.decimal_math import (
    ZERO,
    bps_to_multiplier,
    round_price,
    round_size,
    to_decimal,
)

logger = logging.getLogger(__name__)


@register
class MarketMakingStrategy(BaseStrategy):
    """Market making: post bid and ask, harvest the spread.

    Config keys:
        spread_bps (int):       Half-spread in basis points (default 20)
        inventory_skew (bool):  Skew quotes toward reducing inventory (default True)
        max_inventory (str):    Max absolute base currency position (default "0.1")
        min_spread_bps (int):   Minimum spread before quoting is suppressed (default 5)
        order_size (str):       Fixed quote size in base currency (default "0.001")
    """

    name = "market_making"
    description = "Spread capture with inventory management"

    async def on_start(self) -> None:
        await super().on_start()
        self._spread_bps = int(self.cfg("spread_bps", 20))
        self._inventory_skew = bool(self.cfg("inventory_skew", True))
        self._max_inventory = to_decimal(self.cfg("max_inventory", "0.1"))
        self._min_spread_bps = int(self.cfg("min_spread_bps", 0))
        self._order_size = to_decimal(self.cfg("order_size", "0.001"))
        # Track our current quote order ids so we can cancel before requoting
        self._bid_order_id: str | None = None
        self._ask_order_id: str | None = None
        self._last_quote_ts_ns: int = 0
        self._quote_interval_ns = 500_000_000  # 500ms between requotes

    async def on_tick(self, ctx: StrategyContext) -> None:
        book = ctx.book
        if not book.initialized:
            return

        best_bid = book.best_bid
        best_ask = book.best_ask
        if best_bid is None or best_ask is None:
            return

        # Suppress quoting if spread is already too tight
        if book.spread_in_bps is not None and book.spread_in_bps < self._min_spread_bps:
            return

        # Rate-limit requoting
        if ctx.ts_ns - self._last_quote_ts_ns < self._quote_interval_ns:
            return

        mid = (best_bid + best_ask) / 2

        # Inventory skew
        pos = self._orders.position_tracker.position(ctx.product_id)
        skew = ZERO
        if self._inventory_skew and self._max_inventory > ZERO:
            inventory_ratio = pos.size / self._max_inventory
            skew = inventory_ratio * bps_to_multiplier(self._spread_bps)

        half_spread = bps_to_multiplier(self._spread_bps)
        our_bid = round_price(mid * (1 - half_spread - skew))
        our_ask = round_price(mid * (1 + half_spread - skew))

        # Cancel stale quotes
        if self._bid_order_id:
            await self._orders.cancel_order(self._bid_order_id)
        if self._ask_order_id:
            await self._orders.cancel_order(self._ask_order_id)

        # Check inventory limits before posting
        max_inv = self._max_inventory
        if pos.size < max_inv:
            bid_order = await self._orders.submit_order(
                product_id=ctx.product_id,
                side="buy",
                order_type="limit",
                size=self._order_size,
                limit_price=our_bid,
                book=book,
            )
            self._bid_order_id = bid_order.order_id if bid_order else None

        if pos.size > -max_inv:
            ask_order = await self._orders.submit_order(
                product_id=ctx.product_id,
                side="sell",
                order_type="limit",
                size=self._order_size,
                limit_price=our_ask,
                book=book,
            )
            self._ask_order_id = ask_order.order_id if ask_order else None

        self._last_quote_ts_ns = ctx.ts_ns

        logger.info(
            "MM quote %s bid=%s ask=%s inv=%s",
            ctx.product_id, our_bid, our_ask, pos.size,
        )
