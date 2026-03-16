"""Local L2 order book — maintains a full bid/ask depth from Coinbase WebSocket updates."""

from __future__ import annotations

import logging
from collections import OrderedDict
from decimal import Decimal
from typing import NamedTuple

from coinbase_hft.utils.decimal_math import ZERO, mid_price, spread_bps, to_decimal

logger = logging.getLogger(__name__)


class PriceLevel(NamedTuple):
    price: Decimal
    size: Decimal


class OrderBook:
    """Level-2 order book for a single trading pair.

    Maintains sorted bid (descending) and ask (ascending) price levels.
    Thread-safe reads are safe because asyncio is single-threaded; writes
    must happen from the event loop.
    """

    def __init__(self, product_id: str, depth: int = 50) -> None:
        self.product_id = product_id
        self.depth = depth
        # {price_str: Decimal size}
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._sequence: int = 0
        self._initialized = False

    # ------------------------------------------------------------------
    # Snapshot / delta application
    # ------------------------------------------------------------------

    def mark_initialized(self) -> None:
        """Explicitly mark the book as ready (used after delta-based snapshots)."""
        self._initialized = True

    def apply_snapshot(self, bids: list[list[str]], asks: list[list[str]], sequence: int = 0) -> None:
        """Replace book entirely with a snapshot."""
        self._bids.clear()
        self._asks.clear()
        for price_str, size_str in bids:
            size = to_decimal(size_str)
            if size > ZERO:
                self._bids[to_decimal(price_str)] = size
        for price_str, size_str in asks:
            size = to_decimal(size_str)
            if size > ZERO:
                self._asks[to_decimal(price_str)] = size
        self._sequence = sequence
        self._initialized = True
        logger.debug("%s book snapshot: %d bids, %d asks", self.product_id, len(self._bids), len(self._asks))

    def apply_delta(self, side: str, price_str: str, size_str: str) -> None:
        """Apply a single price-level update. size=0 means remove the level."""
        price = to_decimal(price_str)
        size = to_decimal(size_str)
        book = self._bids if side == "bid" else self._asks
        if size == ZERO:
            book.pop(price, None)
        else:
            book[price] = size

    def apply_deltas(self, updates: list[tuple[str, str, str]]) -> None:
        """Apply a batch of (side, price, size) deltas."""
        for side, price, size in updates:
            self.apply_delta(side, price, size)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def best_bid(self) -> Decimal | None:
        return max(self._bids.keys()) if self._bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self._asks.keys()) if self._asks else None

    @property
    def mid(self) -> Decimal | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return mid_price(bb, ba)

    @property
    def spread(self) -> Decimal | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return ba - bb

    @property
    def spread_in_bps(self) -> Decimal | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return spread_bps(bb, ba)

    def is_crossed(self) -> bool:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return False
        return bb >= ba

    def bids(self, n: int | None = None) -> list[PriceLevel]:
        """Return sorted bid levels (best first = highest price)."""
        levels = sorted(self._bids.items(), reverse=True)
        return [PriceLevel(p, s) for p, s in (levels[:n] if n else levels)]

    def asks(self, n: int | None = None) -> list[PriceLevel]:
        """Return sorted ask levels (best first = lowest price)."""
        levels = sorted(self._asks.items())
        return [PriceLevel(p, s) for p, s in (levels[:n] if n else levels)]

    def bid_size_at(self, price: Decimal) -> Decimal:
        return self._bids.get(price, ZERO)

    def ask_size_at(self, price: Decimal) -> Decimal:
        return self._asks.get(price, ZERO)

    def available_bid_liquidity(self, n_levels: int = 5) -> Decimal:
        """Total bid size across the top N levels."""
        return sum((s for _, s in self.bids(n_levels)), ZERO)

    def available_ask_liquidity(self, n_levels: int = 5) -> Decimal:
        """Total ask size across the top N levels."""
        return sum((s for _, s in self.asks(n_levels)), ZERO)

    def order_imbalance(self, n_levels: int = 5) -> Decimal:
        """Order imbalance ratio: (bid_liq - ask_liq) / (bid_liq + ask_liq)."""
        bid_liq = self.available_bid_liquidity(n_levels)
        ask_liq = self.available_ask_liquidity(n_levels)
        total = bid_liq + ask_liq
        if total == ZERO:
            return ZERO
        return (bid_liq - ask_liq) / total

    def simulate_market_buy(self, size: Decimal) -> list[tuple[Decimal, Decimal]]:
        """Simulate consuming ask levels for a market buy. Returns [(price, filled_size)]."""
        remaining = size
        fills: list[tuple[Decimal, Decimal]] = []
        for level in self.asks():
            if remaining <= ZERO:
                break
            fill = min(remaining, level.size)
            fills.append((level.price, fill))
            remaining -= fill
        return fills

    def simulate_market_sell(self, size: Decimal) -> list[tuple[Decimal, Decimal]]:
        """Simulate consuming bid levels for a market sell. Returns [(price, filled_size)]."""
        remaining = size
        fills: list[tuple[Decimal, Decimal]] = []
        for level in self.bids():
            if remaining <= ZERO:
                break
            fill = min(remaining, level.size)
            fills.append((level.price, fill))
            remaining -= fill
        return fills

    def __repr__(self) -> str:
        bb = self.best_bid
        ba = self.best_ask
        return f"OrderBook({self.product_id} bid={bb} ask={ba} spread={self.spread})"
