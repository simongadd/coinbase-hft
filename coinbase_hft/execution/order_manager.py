"""Unified order interface — routes to paper or live executor transparently.

Strategies call order_manager.submit_order(...) without knowing which mode
they're running in. The OrderManager handles routing, idempotency, and
event coordination.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import EventBus
from coinbase_hft.execution.paper_executor import Order, PaperExecutor
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import to_decimal

logger = logging.getLogger(__name__)


class OrderManager:
    """Single interface for all order operations regardless of trading mode.

    In paper mode: delegates to PaperExecutor
    In live mode:  delegates to LiveExecutor

    Strategies and the risk manager always interact with this class.
    """

    def __init__(
        self,
        mode: str,
        event_bus: EventBus,
        position_tracker: PositionTracker,
        clock: Clock,
        paper_executor: PaperExecutor | None = None,
        live_executor: Any | None = None,
    ) -> None:
        assert mode in ("paper", "live"), f"Invalid mode: {mode}"
        self._mode = mode
        self._bus = event_bus
        self._positions = position_tracker
        self._clock = clock
        self._paper = paper_executor
        self._live = live_executor
        self._pending_client_ids: set[str] = set()

    @property
    def mode(self) -> str:
        return self._mode

    async def submit_order(
        self,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal | str,
        limit_price: Decimal | str | None = None,
        client_order_id: str | None = None,
        book: OrderBook | None = None,
        **kwargs: Any,
    ) -> Order | None:
        """Submit an order through the appropriate executor.

        Returns the Order object, or None if the executor is not available.
        Idempotency: if client_order_id was already submitted this session, skip.
        """
        size = to_decimal(size)
        if limit_price is not None:
            limit_price = to_decimal(limit_price)

        # Idempotency guard
        if client_order_id and client_order_id in self._pending_client_ids:
            logger.warning("Duplicate client_order_id rejected: %s", client_order_id)
            return None
        if client_order_id:
            self._pending_client_ids.add(client_order_id)

        if self._mode == "paper":
            if self._paper is None:
                raise RuntimeError("PaperExecutor not configured")
            return await self._paper.submit_order(
                product_id=product_id,
                side=side,
                order_type=order_type,
                size=size,
                limit_price=limit_price,
                client_order_id=client_order_id,
                book=book,
                **kwargs,
            )
        else:
            if self._live is None:
                raise RuntimeError("LiveExecutor not configured")
            return await self._live.submit_order(
                product_id=product_id,
                side=side,
                order_type=order_type,
                size=size,
                limit_price=limit_price,
                client_order_id=client_order_id,
                **kwargs,
            )

    async def cancel_order(self, order_id: str) -> bool:
        if self._mode == "paper" and self._paper:
            return await self._paper.cancel_order(order_id)
        elif self._live:
            return await self._live.cancel_order(order_id)
        return False

    async def cancel_all_orders(self, product_id: str | None = None) -> int:
        """Cancel all open orders. Used by kill switch."""
        if self._mode == "paper" and self._paper:
            return await self._paper.cancel_all_orders(product_id)
        elif self._live:
            return await self._live.cancel_all_orders(product_id)
        return 0

    async def on_trade_update(self, product_id: str, trade_price: Decimal) -> None:
        """Propagate trade prints to the paper executor for passive maker fill simulation."""
        if self._mode == "paper" and self._paper:
            await self._paper.on_trade_update(product_id, trade_price)

    async def on_book_update(self, product_id: str, book: OrderBook) -> None:
        """Propagate book updates to the paper executor for limit order matching."""
        if self._mode == "paper" and self._paper:
            await self._paper.on_book_update(product_id, book)

    def open_orders(self, product_id: str | None = None) -> list[Order]:
        if self._mode == "paper" and self._paper:
            return self._paper.open_orders(product_id)
        elif self._live:
            return self._live.open_orders(product_id)
        return []

    @property
    def position_tracker(self) -> PositionTracker:
        return self._positions
