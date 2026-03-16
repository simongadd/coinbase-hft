"""Async event bus — decouples components via typed pub/sub."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

class EventType(Enum):
    # Market data
    ORDER_BOOK_SNAPSHOT = auto()
    ORDER_BOOK_DELTA = auto()
    TRADE = auto()
    TICKER = auto()
    CANDLE = auto()

    # Strategy
    SIGNAL = auto()
    ORDER_REQUEST = auto()

    # Execution
    ORDER_SUBMITTED = auto()
    ORDER_FILLED = auto()
    ORDER_PARTIAL_FILL = auto()
    ORDER_CANCELLED = auto()
    ORDER_REJECTED = auto()

    # Risk
    RISK_BREACH = auto()
    CIRCUIT_BREAKER_TRIGGERED = auto()

    # System
    KILL_SWITCH = auto()
    SESSION_START = auto()
    SESSION_END = auto()
    HEARTBEAT = auto()
    WS_CONNECTED = auto()
    WS_DISCONNECTED = auto()


@dataclass
class Event:
    type: EventType
    data: Any
    source: str = ""
    ts_ns: int = field(default_factory=lambda: __import__("time").time_ns())


# ---------------------------------------------------------------------------
# Subscriber callback type
# ---------------------------------------------------------------------------

SubscriberCallback = Callable[[Event], Awaitable[None]]


# ---------------------------------------------------------------------------
# Bus implementation
# ---------------------------------------------------------------------------

class EventBus:
    """Lightweight async pub/sub bus.

    Subscribers register callbacks for specific EventTypes. Publishing an event
    schedules all matching callbacks as asyncio tasks so no subscriber can block
    the publisher.
    """

    def __init__(self) -> None:
        self._subscribers: dict[EventType, list[SubscriberCallback]] = defaultdict(list)
        self._wildcard: list[SubscriberCallback] = []

    def subscribe(self, event_type: EventType, callback: SubscriberCallback) -> None:
        self._subscribers[event_type].append(callback)
        logger.debug("Subscribed %s to %s", callback, event_type.name)

    def subscribe_all(self, callback: SubscriberCallback) -> None:
        """Subscribe to every event type."""
        self._wildcard.append(callback)

    def unsubscribe(self, event_type: EventType, callback: SubscriberCallback) -> None:
        try:
            self._subscribers[event_type].remove(callback)
        except ValueError:
            pass

    async def publish(self, event: Event) -> None:
        """Fire-and-forget: dispatch to all subscribers without awaiting."""
        callbacks = self._subscribers.get(event.type, []) + self._wildcard
        for cb in callbacks:
            asyncio.create_task(self._safe_call(cb, event))

    async def publish_sync(self, event: Event) -> None:
        """Dispatch and await all subscribers in order (use for critical events)."""
        callbacks = self._subscribers.get(event.type, []) + self._wildcard
        for cb in callbacks:
            await self._safe_call(cb, event)

    @staticmethod
    async def _safe_call(cb: SubscriberCallback, event: Event) -> None:
        try:
            await cb(event)
        except Exception:
            logger.exception("Unhandled error in event subscriber %s for %s", cb, event.type.name)
