"""Live order executor — submits real orders to Coinbase Advanced Trade REST API."""

from __future__ import annotations

import asyncio
import logging
import uuid
from decimal import Decimal
from typing import Any

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.execution.paper_executor import Order, OrderSide, OrderStatus, OrderType
from coinbase_hft.utils.decimal_math import ZERO, to_decimal
from coinbase_hft.utils.rate_limiter import TokenBucketRateLimiter
from coinbase_hft.utils.retry import async_retry

logger = logging.getLogger(__name__)


class LiveExecutor:
    """Executes real orders via the Coinbase Advanced Trade REST API.

    Wraps the coinbase-advanced-py SDK for testability. All order operations
    are rate-limited and retried with exponential backoff.
    """

    def __init__(
        self,
        event_bus: EventBus,
        clock: Clock,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        self._bus = event_bus
        self._clock = clock
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(rate=10.0, burst=10.0)
        self._client: Any = None  # Coinbase REST client, injected at startup
        self._orders: dict[str, Order] = {}

    def set_client(self, client: Any) -> None:
        """Inject the Coinbase REST client (enables testing with mocks)."""
        self._client = client

    def _ensure_client(self) -> None:
        if self._client is None:
            raise RuntimeError("Coinbase REST client not initialised — call set_client() first")

    @async_retry(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
    async def submit_order(
        self,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal,
        limit_price: Decimal | None = None,
        client_order_id: str | None = None,
        **kwargs: Any,
    ) -> Order:
        self._ensure_client()
        await self._rate_limiter.acquire()

        cid = client_order_id or str(uuid.uuid4())

        order_config: dict[str, Any] = {}
        if order_type == "market":
            if side == "buy":
                # Market buy: specify quote_size in USD
                notional = to_decimal(kwargs.get("quote_size", size))
                order_config = {"market_market_ioc": {"quote_size": str(notional)}}
            else:
                order_config = {"market_market_ioc": {"base_size": str(size)}}
        else:
            if limit_price is None:
                raise ValueError("limit_price required for limit orders")
            order_config = {
                "limit_limit_gtc": {
                    "base_size": str(size),
                    "limit_price": str(limit_price),
                    "post_only": kwargs.get("post_only", False),
                }
            }

        logger.info(
            "[LIVE] Submitting %s %s %s %s @ %s cid=%s",
            order_type.upper(), side.upper(), size, product_id,
            limit_price or "MARKET", cid,
        )

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._client.create_order(
                client_order_id=cid,
                product_id=product_id,
                side=side.upper(),
                order_configuration=order_config,
            )
        )

        order = self._parse_response(response, cid, product_id, side, order_type, size, limit_price)
        self._orders[order.order_id] = order

        await self._bus.publish(Event(
            type=EventType.ORDER_SUBMITTED,
            data={"order": order, "response": response, "mode": "live"},
            source="live_executor",
            ts_ns=self._clock.now_ns(),
        ))

        return order

    def _parse_response(
        self,
        response: Any,
        cid: str,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal,
        limit_price: Decimal | None,
    ) -> Order:
        """Parse Coinbase API response into an Order object."""
        success = getattr(response, "success", False)
        if not success:
            reason = getattr(response, "error_response", {})
            logger.error("[LIVE] Order rejected: %s", reason)
            return Order(
                order_id=str(uuid.uuid4()),
                client_order_id=cid,
                product_id=product_id,
                side=OrderSide(side),
                order_type=OrderType(order_type),
                size=size,
                limit_price=limit_price,
                status=OrderStatus.REJECTED,
            )

        resp = getattr(response, "success_response", {})
        order_id = getattr(resp, "order_id", str(uuid.uuid4())) if hasattr(resp, "order_id") else resp.get("order_id", str(uuid.uuid4()))

        return Order(
            order_id=str(order_id),
            client_order_id=cid,
            product_id=product_id,
            side=OrderSide(side),
            order_type=OrderType(order_type),
            size=size,
            limit_price=limit_price,
            status=OrderStatus.OPEN,
            created_ts_ns=self._clock.now_ns(),
        )

    @async_retry(max_attempts=3, base_delay=0.5)
    async def cancel_order(self, order_id: str) -> bool:
        self._ensure_client()
        await self._rate_limiter.acquire()
        logger.info("[LIVE] Cancelling order %s", order_id)
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.cancel_orders(order_ids=[order_id]),
            )
            if order_id in self._orders:
                self._orders[order_id].status = OrderStatus.CANCELLED
            await self._bus.publish(Event(
                type=EventType.ORDER_CANCELLED,
                data={"order_id": order_id, "mode": "live"},
                source="live_executor",
                ts_ns=self._clock.now_ns(),
            ))
            return True
        except Exception as exc:
            logger.error("[LIVE] Failed to cancel order %s: %s", order_id, exc)
            return False

    async def cancel_all_orders(self, product_id: str | None = None) -> int:
        open_orders = [
            o for o in self._orders.values()
            if o.status == OrderStatus.OPEN
            and (product_id is None or o.product_id == product_id)
        ]
        if not open_orders:
            return 0

        self._ensure_client()
        await self._rate_limiter.acquire()

        order_ids = [o.order_id for o in open_orders]
        logger.info("[LIVE] Batch cancelling %d orders", len(order_ids))

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.cancel_orders(order_ids=order_ids),
            )
        except Exception as exc:
            logger.error("[LIVE] Batch cancel failed: %s", exc)

        for order in open_orders:
            order.status = OrderStatus.CANCELLED

        return len(order_ids)

    def open_orders(self, product_id: str | None = None) -> list[Order]:
        return [
            o for o in self._orders.values()
            if o.status == OrderStatus.OPEN
            and (product_id is None or o.product_id == product_id)
        ]

