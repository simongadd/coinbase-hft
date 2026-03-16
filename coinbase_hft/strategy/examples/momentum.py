"""Momentum strategy: short-term price momentum with RSI filter."""

from __future__ import annotations

import logging
from decimal import Decimal

from coinbase_hft.strategy.base_strategy import BaseStrategy, StrategyContext
from coinbase_hft.strategy.strategy_registry import register
from coinbase_hft.strategy.signals.technical import ema_crossover, rsi
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


@register
class MomentumStrategy(BaseStrategy):
    """Momentum / mean-reversion strategy driven by EMA crossover + RSI filter.

    Config keys:
        lookback_periods (int): Number of closed candles to consider (default 20)
        entry_threshold (str):  Minimum EMA divergence fraction to enter (default "0.002")
        fast_ema (int):         Fast EMA period (default 9)
        slow_ema (int):         Slow EMA period (default 21)
        rsi_period (int):       RSI calculation period (default 14)
        rsi_overbought (int):   RSI level to suppress buys (default 70)
        rsi_oversold (int):     RSI level to suppress sells (default 30)
        order_size (str):       Base currency position size (default "0.001")
        candle_interval (int):  Seconds per candle used for signals (default 60)
    """

    name = "momentum"
    description = "EMA crossover momentum with RSI filter"

    async def on_start(self) -> None:
        await super().on_start()
        self._lookback = int(self.cfg("lookback_periods", 20))
        self._fast_ema = int(self.cfg("fast_ema", 9))
        self._slow_ema = int(self.cfg("slow_ema", 21))
        self._rsi_period = int(self.cfg("rsi_period", 14))
        self._rsi_ob = int(self.cfg("rsi_overbought", 70))
        self._rsi_os = int(self.cfg("rsi_oversold", 30))
        self._order_size = to_decimal(self.cfg("order_size", "0.001"))
        self._candle_interval = int(self.cfg("candle_interval", 60))
        self._in_position: dict[str, str | None] = {pid: None for pid in self.product_ids}
        self._last_signal_ts: dict[str, int] = {}

    async def on_tick(self, ctx: StrategyContext) -> None:
        pid = ctx.product_id
        closes = self._store.close_prices(pid, self._candle_interval, self._lookback + 5)

        if len(closes) < max(self._slow_ema + 2, self._rsi_period + 2):
            return

        # Throttle: max one signal per candle
        candles = self._store.candles(pid, self._candle_interval, 1)
        if not candles:
            return
        last_candle_ts = candles[-1].open_time
        if self._last_signal_ts.get(pid, 0) >= last_candle_ts:
            return

        crossover = ema_crossover(closes, self._fast_ema, self._slow_ema)
        rsi_val = rsi(closes, self._rsi_period)
        current_side = self._in_position[pid]

        if crossover is None:
            return

        book = ctx.book
        if not book.initialized:
            return

        if crossover == 1 and current_side != "long":
            # Bullish crossover — enter long if RSI not overbought
            if rsi_val is not None and rsi_val > self._rsi_ob:
                return
            # Close short if any
            if current_side == "short":
                await self._orders.submit_order(
                    product_id=pid, side="buy", order_type="market",
                    size=self._order_size, book=book,
                )
            await self._orders.submit_order(
                product_id=pid, side="buy", order_type="market",
                size=self._order_size, book=book,
            )
            self._in_position[pid] = "long"
            self._last_signal_ts[pid] = last_candle_ts
            logger.info("MOMENTUM BUY %s rsi=%.1f", pid, rsi_val or 0)

        elif crossover == -1 and current_side != "short":
            # Bearish crossover — enter short if RSI not oversold
            if rsi_val is not None and rsi_val < self._rsi_os:
                return
            if current_side == "long":
                await self._orders.submit_order(
                    product_id=pid, side="sell", order_type="market",
                    size=self._order_size, book=book,
                )
            await self._orders.submit_order(
                product_id=pid, side="sell", order_type="market",
                size=self._order_size, book=book,
            )
            self._in_position[pid] = "short"
            self._last_signal_ts[pid] = last_candle_ts
            logger.info("MOMENTUM SELL %s rsi=%.1f", pid, rsi_val or 0)
