"""Realistic fill simulation model for paper trading.

Models:
- Market orders: immediate fill at current best bid/ask + slippage
- Limit orders: fill probability based on price crossing + queue position
- Partial fills based on available book liquidity
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto

from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import (
    ZERO,
    apply_slippage_buy,
    apply_slippage_sell,
    fee_amount,
    to_decimal,
    weighted_average_price,
)


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"


@dataclass
class FillResult:
    order_id: str
    filled_size: Decimal = ZERO
    avg_fill_price: Decimal = ZERO
    fee: Decimal = ZERO
    is_partial: bool = False
    fills: list[tuple[Decimal, Decimal]] = field(default_factory=list)  # [(price, size)]

    @property
    def is_filled(self) -> bool:
        return self.filled_size > ZERO

    @property
    def notional(self) -> Decimal:
        return self.avg_fill_price * self.filled_size


class FillModel:
    """Configurable fill simulation for paper trading.

    Supports realistic slippage, queue position, partial fills, and
    fee application using Coinbase's actual fee schedule.
    """

    def __init__(
        self,
        slippage_bps: int = 5,
        fill_probability: Decimal | float = Decimal("0.85"),
        fee_rate: Decimal | float = Decimal("0.006"),
        queue_position_factor: Decimal | float = Decimal("0.3"),
    ) -> None:
        self.slippage_bps = slippage_bps
        self.fill_probability = to_decimal(fill_probability)
        self.fee_rate = to_decimal(fee_rate)
        # Fraction of the resting quantity assumed to be ahead in queue
        self.queue_position_factor = to_decimal(queue_position_factor)

    def simulate_market_fill(
        self,
        order_id: str,
        side: OrderSide,
        size: Decimal,
        book: OrderBook,
    ) -> FillResult:
        """Simulate a market order against the live order book."""
        if side == OrderSide.BUY:
            fills = book.simulate_market_buy(size)
            # Apply slippage to each fill level
            fills = [(apply_slippage_buy(p, self.slippage_bps), s) for p, s in fills]
        else:
            fills = book.simulate_market_sell(size)
            fills = [(apply_slippage_sell(p, self.slippage_bps), s) for p, s in fills]

        filled_size = sum(s for _, s in fills)
        avg_price = weighted_average_price(fills) if fills else ZERO
        notional = avg_price * filled_size
        fee = fee_amount(notional, self.fee_rate)

        return FillResult(
            order_id=order_id,
            filled_size=filled_size,
            avg_fill_price=avg_price,
            fee=fee,
            is_partial=filled_size < size,
            fills=fills,
        )

    def simulate_limit_fill(
        self,
        order_id: str,
        side: OrderSide,
        size: Decimal,
        limit_price: Decimal,
        book: OrderBook,
    ) -> FillResult:
        """Simulate a passive maker limit order fill attempt.

        A passive buy fills when we are the best bid (limit_price >= best_bid).
        A passive sell fills when we are the best ask (limit_price <= best_ask).
        Fill probability represents the chance we are at the front of the queue.
        """
        best_bid = book.best_bid
        best_ask = book.best_ask

        # Check if we are price-competitive (at or better than the exchange's best)
        if side == OrderSide.BUY:
            if best_bid is None or limit_price < best_bid:
                return FillResult(order_id=order_id)  # behind queue, no fill
        else:
            if best_ask is None or limit_price > best_ask:
                return FillResult(order_id=order_id)  # behind queue, no fill

        # Apply fill probability (queue position model)
        if random.random() > float(self.fill_probability):
            return FillResult(order_id=order_id)  # not filled this tick

        # Maker fill: our order is at the best bid/ask level.
        # Available size = the liquidity on the other side at our price level.
        if side == OrderSide.BUY and best_bid is not None:
            available = book.bid_size_at(best_bid)
            available_to_us = available * (1 - self.queue_position_factor)
        elif best_ask is not None:
            available = book.ask_size_at(best_ask)
            available_to_us = available * (1 - self.queue_position_factor)
        else:
            available_to_us = size

        filled_size = min(size, max(ZERO, available_to_us))
        if filled_size <= ZERO:
            return FillResult(order_id=order_id)

        fill_price = limit_price
        notional = fill_price * filled_size
        fee = fee_amount(notional, self.fee_rate)

        return FillResult(
            order_id=order_id,
            filled_size=filled_size,
            avg_fill_price=fill_price,
            fee=fee,
            is_partial=filled_size < size,
            fills=[(fill_price, filled_size)],
        )

    def simulate_trade_fill(
        self,
        order_id: str,
        side: OrderSide,
        size: Decimal,
        limit_price: Decimal,
        trade_price: Decimal,
    ) -> FillResult:
        """Simulate a passive maker fill when a market trade occurs at trade_price.

        A resting buy fills if a trade ticks at or below our limit price.
        A resting sell fills if a trade ticks at or above our limit price.
        """
        if side == OrderSide.BUY and trade_price > limit_price:
            return FillResult(order_id=order_id)
        if side == OrderSide.SELL and trade_price < limit_price:
            return FillResult(order_id=order_id)

        if random.random() > float(self.fill_probability):
            return FillResult(order_id=order_id)

        notional = limit_price * size
        fee = fee_amount(notional, self.fee_rate)
        return FillResult(
            order_id=order_id,
            filled_size=size,
            avg_fill_price=limit_price,
            fee=fee,
            is_partial=False,
            fills=[(limit_price, size)],
        )
