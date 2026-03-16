"""Abstract base class all trading strategies must implement."""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from coinbase_hft.core.clock import Clock
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.market_data.market_data_store import MarketDataStore
from coinbase_hft.market_data.order_book import OrderBook

logger = logging.getLogger(__name__)


@dataclass
class StrategyContext:
    """Immutable snapshot of market state passed to each strategy tick."""
    product_id: str
    book: OrderBook
    data_store: MarketDataStore
    ts_ns: int
    extra: dict[str, Any] = field(default_factory=dict)


class BaseStrategy(abc.ABC):
    """All strategies inherit from this class.

    Strategies receive market data snapshots and return order intents
    through the OrderManager. They never touch the network directly.

    Lifecycle:
        on_start()  — called once before the first tick
        on_tick()   — called on every market data event
        on_fill()   — called when one of our orders fills
        on_stop()   — called on clean shutdown
    """

    #: Override in subclass — used for auto-discovery and CLI selection
    name: str = "base"
    description: str = ""

    def __init__(
        self,
        product_ids: list[str],
        order_manager: OrderManager,
        data_store: MarketDataStore,
        clock: Clock,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.product_ids = product_ids
        self._orders = order_manager
        self._store = data_store
        self._clock = clock
        self.config: dict[str, Any] = config or {}
        self._running = False
        self.logger = logging.getLogger(f"coinbase_hft.strategy.{self.name}")

    # ------------------------------------------------------------------
    # Lifecycle hooks (override as needed)
    # ------------------------------------------------------------------

    async def on_start(self) -> None:
        """Called once when the engine starts. Initialise state here."""
        self._running = True
        self.logger.info("Strategy '%s' started for %s", self.name, self.product_ids)

    @abc.abstractmethod
    async def on_tick(self, ctx: StrategyContext) -> None:
        """Called on every market data update.

        Submit orders via self._orders.submit_order(). Do not call external
        APIs or perform blocking I/O in this method.
        """

    async def on_fill(self, order: Any, fill: Any) -> None:
        """Called when one of our orders is filled or partially filled."""

    async def on_stop(self) -> None:
        """Called on shutdown. Cancel any open orders here."""
        self._running = False
        for pid in self.product_ids:
            cancelled = await self._orders.cancel_all_orders(pid)
            if cancelled:
                self.logger.info("Cancelled %d orders on stop", cancelled)

    # ------------------------------------------------------------------
    # Helpers available to all strategies
    # ------------------------------------------------------------------

    def mid_price(self, product_id: str) -> Decimal | None:
        ticker = self._store.ticker(product_id)
        if ticker:
            from coinbase_hft.utils.decimal_math import mid_price
            return mid_price(ticker.best_bid, ticker.best_ask)
        return None

    def best_bid(self, product_id: str) -> Decimal | None:
        t = self._store.ticker(product_id)
        return t.best_bid if t else None

    def best_ask(self, product_id: str) -> Decimal | None:
        t = self._store.ticker(product_id)
        return t.best_ask if t else None

    def cfg(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)
