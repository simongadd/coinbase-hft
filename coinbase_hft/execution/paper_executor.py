"""Paper trading executor — simulates order fills against real market data."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from typing import Any

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.execution.fill_model import FillModel, FillResult, OrderSide, OrderType
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import ZERO, to_decimal

logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    PENDING = auto()
    OPEN = auto()
    FILLED = auto()
    PARTIALLY_FILLED = auto()
    CANCELLED = auto()
    REJECTED = auto()


@dataclass
class Order:
    order_id: str
    client_order_id: str
    product_id: str
    side: OrderSide
    order_type: OrderType
    size: Decimal
    limit_price: Decimal | None = None
    status: OrderStatus = OrderStatus.PENDING
    filled_size: Decimal = ZERO
    avg_fill_price: Decimal = ZERO
    fees_paid: Decimal = ZERO
    created_ts_ns: int = 0
    filled_ts_ns: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def remaining_size(self) -> Decimal:
        return self.size - self.filled_size

    @property
    def is_terminal(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


class PaperExecutor:
    """Executes orders in simulation against live order book data.

    Market orders are filled immediately on the next tick.
    Limit orders queue and are checked on each book update.
    """

    def __init__(
        self,
        fill_model: FillModel,
        position_tracker: PositionTracker,
        event_bus: EventBus,
        clock: Clock,
    ) -> None:
        self._model = fill_model
        self._positions = position_tracker
        self._bus = event_bus
        self._clock = clock
        self._orders: dict[str, Order] = {}
        self._open_limit_orders: list[str] = []  # order_ids

    async def submit_order(
        self,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal,
        limit_price: Decimal | None = None,
        client_order_id: str | None = None,
        book: OrderBook | None = None,
        **kwargs: Any,
    ) -> Order:
        post_only: bool = kwargs.get("post_only", False)
        order_id = str(uuid.uuid4())
        cid = client_order_id or str(uuid.uuid4())
        order = Order(
            order_id=order_id,
            client_order_id=cid,
            product_id=product_id,
            side=OrderSide(side),
            order_type=OrderType(order_type),
            size=to_decimal(size),
            limit_price=to_decimal(limit_price) if limit_price is not None else None,
            status=OrderStatus.OPEN,
            created_ts_ns=self._clock.now_ns(),
            extra={"post_only": post_only},
        )
        self._orders[order_id] = order

        # Post-only rejection: if the order would immediately cross the book, reject it
        if post_only and order.order_type == OrderType.LIMIT and book is not None:
            if order.side == OrderSide.BUY and book.best_ask is not None:
                if order.limit_price >= book.best_ask:
                    order.status = OrderStatus.REJECTED
                    await self._bus.publish(Event(
                        type=EventType.ORDER_REJECTED,
                        data={"order": order, "mode": "paper", "reason": "post_only"},
                        source="paper_executor",
                        ts_ns=order.created_ts_ns,
                    ))
                    logger.info("[PAPER] Post-only order REJECTED (would cross) %s %s @ %s", side.upper(), product_id, limit_price)
                    return order
            elif order.side == OrderSide.SELL and book.best_bid is not None:
                if order.limit_price <= book.best_bid:
                    order.status = OrderStatus.REJECTED
                    await self._bus.publish(Event(
                        type=EventType.ORDER_REJECTED,
                        data={"order": order, "mode": "paper", "reason": "post_only"},
                        source="paper_executor",
                        ts_ns=order.created_ts_ns,
                    ))
                    logger.info("[PAPER] Post-only order REJECTED (would cross) %s %s @ %s", side.upper(), product_id, limit_price)
                    return order

        logger.info(
            "[PAPER] Order submitted %s %s %s %s @ %s",
            order_type.upper(), side.upper(), size, product_id,
            limit_price or "MARKET",
        )

        await self._bus.publish(Event(
            type=EventType.ORDER_SUBMITTED,
            data={"order": order, "mode": "paper"},
            source="paper_executor",
            ts_ns=order.created_ts_ns,
        ))

        if order_type == "market":
            if book:
                await self._fill_market_order(order, book)
            # Market orders with no book will be filled on next tick
        else:
            self._open_limit_orders.append(order_id)

        return order

    async def on_trade_update(self, product_id: str, trade_price: Decimal) -> None:
        """Called on every market trade — fills resting limit orders that the trade ticks through."""
        to_remove: list[str] = []
        for order_id in list(self._open_limit_orders):
            order = self._orders.get(order_id)
            if not order or order.is_terminal:
                to_remove.append(order_id)
                continue
            if order.product_id != product_id:
                continue
            if order.limit_price is None:
                continue
            result = self._model.simulate_trade_fill(
                order_id=order_id,
                side=order.side,
                size=order.remaining_size,
                limit_price=order.limit_price,
                trade_price=trade_price,
            )
            if result.is_filled:
                await self._apply_fill(order, result)
                if order.status == OrderStatus.FILLED:
                    to_remove.append(order_id)
        for oid in to_remove:
            if oid in self._open_limit_orders:
                self._open_limit_orders.remove(oid)

    async def on_book_update(self, product_id: str, book: OrderBook) -> None:
        """Called on every order book update — attempts to fill pending limit orders."""
        to_remove: list[str] = []
        for order_id in list(self._open_limit_orders):
            order = self._orders.get(order_id)
            if not order or order.is_terminal:
                to_remove.append(order_id)
                continue
            if order.product_id != product_id:
                continue
            result = self._model.simulate_limit_fill(
                order_id=order_id,
                side=order.side,
                size=order.remaining_size,
                limit_price=order.limit_price,  # type: ignore[arg-type]
                book=book,
            )
            if result.is_filled:
                await self._apply_fill(order, result)
                if order.status == OrderStatus.FILLED:
                    to_remove.append(order_id)
        for oid in to_remove:
            if oid in self._open_limit_orders:
                self._open_limit_orders.remove(oid)

    async def _fill_market_order(self, order: Order, book: OrderBook) -> None:
        result = self._model.simulate_market_fill(
            order_id=order.order_id,
            side=order.side,
            size=order.size,
            book=book,
        )
        if result.is_filled:
            await self._apply_fill(order, result)

    async def _apply_fill(self, order: Order, result: FillResult) -> None:
        order.filled_size += result.filled_size
        order.fees_paid += result.fee
        order.filled_ts_ns = self._clock.now_ns()

        # Compute new weighted average fill price
        if order.avg_fill_price == ZERO:
            order.avg_fill_price = result.avg_fill_price
        else:
            total_notional = order.avg_fill_price * (order.filled_size - result.filled_size)
            total_notional += result.avg_fill_price * result.filled_size
            order.avg_fill_price = total_notional / order.filled_size

        is_fully_filled = order.filled_size >= order.size
        order.status = OrderStatus.FILLED if is_fully_filled else OrderStatus.PARTIALLY_FILLED

        # Update position tracker
        self._positions.on_fill(
            product_id=order.product_id,
            side=order.side.value,
            filled_size=result.filled_size,
            avg_fill_price=result.avg_fill_price,
            fee=result.fee,
        )

        ev_type = EventType.ORDER_FILLED if is_fully_filled else EventType.ORDER_PARTIAL_FILL
        await self._bus.publish(Event(
            type=ev_type,
            data={"order": order, "fill": result, "mode": "paper"},
            source="paper_executor",
            ts_ns=order.filled_ts_ns,
        ))

        logger.info(
            "[PAPER] Fill %s %s %s %s avg=%.4f fee=%.6f status=%s",
            order.side.value.upper(),
            result.filled_size, order.product_id,
            order.order_type.value.upper(),
            result.avg_fill_price,
            result.fee,
            order.status.name,
        )

    async def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if not order or order.is_terminal:
            return False
        order.status = OrderStatus.CANCELLED
        if order_id in self._open_limit_orders:
            self._open_limit_orders.remove(order_id)
        await self._bus.publish(Event(
            type=EventType.ORDER_CANCELLED,
            data={"order": order, "mode": "paper"},
            source="paper_executor",
            ts_ns=self._clock.now_ns(),
        ))
        logger.info("[PAPER] Order cancelled %s", order_id)
        return True

    async def cancel_all_orders(self, product_id: str | None = None) -> int:
        count = 0
        for order_id in list(self._open_limit_orders):
            order = self._orders.get(order_id)
            if order and (product_id is None or order.product_id == product_id):
                await self.cancel_order(order_id)
                count += 1
        return count

    def get_order(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)

    def open_orders(self, product_id: str | None = None) -> list[Order]:
        orders = [
            self._orders[oid] for oid in self._open_limit_orders
            if oid in self._orders
        ]
        if product_id:
            orders = [o for o in orders if o.product_id == product_id]
        return orders
