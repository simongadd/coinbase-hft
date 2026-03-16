"""Coinbase Advanced Trade WebSocket consumer with auto-reconnect."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

import jwt
import websockets
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from websockets.exceptions import ConnectionClosed

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import Event, EventBus, EventType
from coinbase_hft.market_data.candle_aggregator import CandleAggregator
from coinbase_hft.market_data.market_data_store import MarketDataStore, TickerSnapshot, TradeEvent
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import to_decimal
from coinbase_hft.utils.retry import async_retry

logger = logging.getLogger(__name__)

WS_URL = "wss://advanced-trade-ws.coinbase.com"


def _build_jwt(api_key: str, api_secret: str) -> str:
    """Build a JWT token for Coinbase Advanced Trade WebSocket authentication."""
    private_key = load_pem_private_key(api_secret.encode("utf-8"), password=None)
    now = int(time.time())
    payload = {
        "sub": api_key,
        "iss": "coinbase-cloud",
        "nbf": now,
        "exp": now + 120,
        "aud": ["public_websocket_api"],
    }
    return jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers={"kid": api_key, "nonce": secrets.token_hex()},
    )


class CoinbaseWebSocketFeed:
    """Manages WebSocket connections to Coinbase and dispatches parsed events.

    Handles:
      - level2 (order book snapshots + deltas)
      - market_trades
      - ticker
      - user (fills — requires auth, live mode only)
    """

    def __init__(
        self,
        product_ids: list[str],
        event_bus: EventBus,
        data_store: MarketDataStore,
        clock: Clock,
        channels: list[str] | None = None,
        subscribe_user: bool = False,
    ) -> None:
        self.product_ids = product_ids
        self._bus = event_bus
        self._store = data_store
        self._clock = clock
        self._channels = channels or ["level2", "market_trades", "ticker"]
        self._subscribe_user = subscribe_user
        self._books: dict[str, OrderBook] = {pid: OrderBook(pid) for pid in product_ids}
        self._candles: dict[str, CandleAggregator] = {
            pid: CandleAggregator(pid, interval_seconds=60) for pid in product_ids
        }
        self._ws: Any = None
        self._running = False
        self._last_message_ts: float = 0.0

    @property
    def order_books(self) -> dict[str, OrderBook]:
        return self._books

    @property
    def last_message_ts(self) -> float:
        return self._last_message_ts

    async def start(self) -> None:
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._run_connection()
                backoff = 1.0  # reset on clean disconnect
            except Exception as exc:
                if not self._running:
                    break
                logger.warning("WebSocket disconnected: %s — reconnecting in %.1fs", exc, backoff)
                await self._bus.publish(Event(
                    type=EventType.WS_DISCONNECTED,
                    data={"reason": str(exc)},
                    source="websocket_feed",
                ))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _run_connection(self) -> None:
        logger.info("Connecting to Coinbase WebSocket: %s", WS_URL)
        async with websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=10 * 1024 * 1024,  # 10MB — L2 snapshots can be large
        ) as ws:
            self._ws = ws
            await self._subscribe(ws)
            await self._bus.publish(Event(
                type=EventType.WS_CONNECTED,
                data={"url": WS_URL},
                source="websocket_feed",
            ))
            logger.info("WebSocket connected and subscribed")
            async for raw in ws:
                self._last_message_ts = time.monotonic()
                await self._dispatch(json.loads(raw))

    async def _subscribe(self, ws: Any) -> None:
        api_key = os.environ.get("COINBASE_API_KEY", "")
        api_secret = os.environ.get("COINBASE_API_SECRET", "")

        channels = list(self._channels)
        if self._subscribe_user:
            channels.append("user")

        for channel in channels:
            payload: dict[str, Any] = {
                "type": "subscribe",
                "channel": channel,
                "product_ids": self.product_ids,
            }
            if api_key and api_secret:
                payload["jwt"] = _build_jwt(api_key, api_secret)
            await ws.send(json.dumps(payload))
            logger.debug("Subscribed to channel: %s", channel)

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        # Record latency from Coinbase-side timestamp if present
        cb_ts = msg.get("timestamp")
        if cb_ts:
            try:
                import datetime
                cb_dt = datetime.datetime.fromisoformat(cb_ts.replace("Z", "+00:00"))
                latency_ms = (datetime.datetime.now(datetime.timezone.utc) - cb_dt).total_seconds() * 1000
                self._store.record_latency_sample(latency_ms)
            except Exception:
                pass

        channel = msg.get("channel", "")
        events = msg.get("events", [])
        if msg.get("type") == "error":
            logger.warning("WS error: %s", msg.get("message", msg))

        for event in events:
            if channel == "l2_data":
                await self._handle_l2(event)
            elif channel == "market_trades":
                await self._handle_trades(event)
            elif channel == "ticker":
                await self._handle_ticker(event)
            elif channel == "user":
                await self._handle_user(event)

    async def _handle_l2(self, event: dict[str, Any]) -> None:
        # Coinbase l2_data event structure:
        # {"type": "snapshot"|"update", "product_id": "BTC-USD", "updates": [...]}
        event_type = event.get("type")  # "snapshot" or "update"
        pid = event.get("product_id")
        if not pid or pid not in self._books:
            return

        book = self._books[pid]

        if event_type == "snapshot":
            book._bids.clear()
            book._asks.clear()

        for update in event.get("updates", []):
            side = update.get("side", "").lower()
            price = update.get("price_level", "0")
            size = update.get("new_quantity", "0")
            book.apply_delta(side, price, size)

        if event_type == "snapshot":
            book.mark_initialized()
            logger.debug("Book initialized: %s bids=%d asks=%d", pid, len(book._bids), len(book._asks))

        if not book.initialized:
            return

        ev_type = EventType.ORDER_BOOK_SNAPSHOT if event_type == "snapshot" else EventType.ORDER_BOOK_DELTA
        await self._bus.publish(Event(
            type=ev_type,
            data={"product_id": pid, "book": book},
            source="websocket_feed",
            ts_ns=self._clock.now_ns(),
        ))

    async def _handle_trades(self, event: dict[str, Any]) -> None:
        for trade in event.get("trades", []):
            pid = trade.get("product_id")
            if not pid:
                continue
            ts_ns = self._clock.now_ns()
            te = TradeEvent(
                product_id=pid,
                price=to_decimal(trade.get("price", "0")),
                size=to_decimal(trade.get("size", "0")),
                side=trade.get("side", "unknown").lower(),
                trade_id=trade.get("trade_id", ""),
                ts_ns=ts_ns,
            )
            self._store.add_trade(te)

            # Feed candle aggregator
            if pid in self._candles:
                closed = await self._candles[pid].on_trade(
                    trade.get("price", "0"),
                    trade.get("size", "0"),
                    ts_ns,
                )
                if closed:
                    self._store.add_candle(closed)
                    await self._bus.publish(Event(
                        type=EventType.CANDLE,
                        data={"candle": closed},
                        source="websocket_feed",
                        ts_ns=ts_ns,
                    ))

            # publish_sync so fill checks run inline before orders can be cancelled
            await self._bus.publish_sync(Event(
                type=EventType.TRADE,
                data=te,
                source="websocket_feed",
                ts_ns=ts_ns,
            ))

    async def _handle_ticker(self, event: dict[str, Any]) -> None:
        for tick in event.get("tickers", []):
            pid = tick.get("product_id")
            if not pid:
                continue
            ts_ns = self._clock.now_ns()
            snap = TickerSnapshot(
                product_id=pid,
                price=to_decimal(tick.get("price", "0")),
                best_bid=to_decimal(tick.get("best_bid", "0")),
                best_ask=to_decimal(tick.get("best_ask", "0")),
                volume_24h=to_decimal(tick.get("volume_24_h", "0")),
                ts_ns=ts_ns,
            )
            self._store.update_ticker(snap)
            await self._bus.publish(Event(
                type=EventType.TICKER,
                data=snap,
                source="websocket_feed",
                ts_ns=ts_ns,
            ))

    async def _handle_user(self, event: dict[str, Any]) -> None:
        """Handle live order fills/cancellations from the user channel."""
        for order in event.get("orders", []):
            status = order.get("status", "")
            if status == "FILLED":
                await self._bus.publish(Event(
                    type=EventType.ORDER_FILLED,
                    data=order,
                    source="user_channel",
                    ts_ns=self._clock.now_ns(),
                ))
            elif status == "CANCELLED":
                await self._bus.publish(Event(
                    type=EventType.ORDER_CANCELLED,
                    data=order,
                    source="user_channel",
                    ts_ns=self._clock.now_ns(),
                ))
