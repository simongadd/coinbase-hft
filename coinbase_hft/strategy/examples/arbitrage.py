"""Cross-pair statistical arbitrage strategy stub.

Monitors spread between two correlated pairs (e.g., BTC-USD and ETH-USD
normalised by a ratio) and trades when spread deviates beyond threshold.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from coinbase_hft.strategy.base_strategy import BaseStrategy, StrategyContext
from coinbase_hft.strategy.strategy_registry import register
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


@register
class ArbitrageStrategy(BaseStrategy):
    """Cross-pair spread arbitrage.

    Config keys:
        pair_a (str):           First pair (default "BTC-USD")
        pair_b (str):           Second pair (default "ETH-USD")
        ratio (str):            Historical price ratio pair_a/pair_b (default "14.0")
        min_spread_bps (int):   Minimum deviation in bps to trigger entry (default 10)
        max_position_usd (str): Max notional per leg (default "500.00")
        z_score_window (int):   Rolling window for z-score (default 30)
    """

    name = "arbitrage"
    description = "Cross-pair statistical spread arbitrage"

    async def on_start(self) -> None:
        await super().on_start()
        self._pair_a = self.cfg("pair_a", "BTC-USD")
        self._pair_b = self.cfg("pair_b", "ETH-USD")
        self._ratio = to_decimal(self.cfg("ratio", "14.0"))
        self._min_spread_bps = int(self.cfg("min_spread_bps", 10))
        self._max_position_usd = to_decimal(self.cfg("max_position_usd", "500.00"))
        self._z_window = int(self.cfg("z_score_window", 30))
        self._spread_history: list[Decimal] = []
        self._in_trade = False

    async def on_tick(self, ctx: StrategyContext) -> None:
        # Only act when we receive a tick for pair_a
        if ctx.product_id != self._pair_a:
            return

        ticker_a = self._store.ticker(self._pair_a)
        ticker_b = self._store.ticker(self._pair_b)

        if not ticker_a or not ticker_b:
            return
        if ticker_a.price == ZERO or ticker_b.price == ZERO:
            return

        # Normalised spread: pair_a / (pair_b * ratio) - 1
        synthetic = ticker_b.price * self._ratio
        spread = (ticker_a.price / synthetic) - Decimal("1")
        self._spread_history.append(spread)
        if len(self._spread_history) > self._z_window:
            self._spread_history.pop(0)

        if len(self._spread_history) < self._z_window:
            return

        mean = sum(self._spread_history) / Decimal(len(self._spread_history))
        variance = sum((s - mean) ** 2 for s in self._spread_history) / Decimal(len(self._spread_history))
        std = variance.sqrt()
        if std == ZERO:
            return

        z_score = (spread - mean) / std
        min_spread = to_decimal(self._min_spread_bps) / Decimal("10000")

        if not self._in_trade:
            if z_score > Decimal("2"):
                # Pair A is expensive relative to B — sell A, buy B
                size_a = (self._max_position_usd / ticker_a.price)
                size_b = (self._max_position_usd / ticker_b.price)
                await self._orders.submit_order(
                    product_id=self._pair_a, side="sell", order_type="market",
                    size=size_a, book=ctx.book,
                )
                logger.info("ARB ENTRY: sell %s z=%.2f", self._pair_a, z_score)
                self._in_trade = True
            elif z_score < Decimal("-2"):
                # Pair A is cheap relative to B — buy A, sell B
                size_a = (self._max_position_usd / ticker_a.price)
                await self._orders.submit_order(
                    product_id=self._pair_a, side="buy", order_type="market",
                    size=size_a, book=ctx.book,
                )
                logger.info("ARB ENTRY: buy %s z=%.2f", self._pair_a, z_score)
                self._in_trade = True
        else:
            # Exit when z-score reverts toward mean
            if abs(z_score) < Decimal("0.5"):
                pos = self._orders.position_tracker.position(self._pair_a)
                if pos.size != ZERO:
                    side = "sell" if pos.size > ZERO else "buy"
                    await self._orders.submit_order(
                        product_id=self._pair_a, side=side, order_type="market",
                        size=abs(pos.size), book=ctx.book,
                    )
                    logger.info("ARB EXIT: %s z=%.2f", side, z_score)
                self._in_trade = False
