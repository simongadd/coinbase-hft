"""Microstructure signals: order flow imbalance, trade intensity, VPIN."""

from __future__ import annotations

from decimal import Decimal

from coinbase_hft.market_data.market_data_store import MarketDataStore, TradeEvent
from coinbase_hft.market_data.order_book import OrderBook
from coinbase_hft.utils.decimal_math import ZERO


def order_flow_imbalance(book: OrderBook, n_levels: int = 5) -> Decimal:
    """OFI: (bid_volume - ask_volume) / (bid_volume + ask_volume).

    Positive value signals buy pressure; negative signals sell pressure.
    """
    return book.order_imbalance(n_levels)


def trade_intensity(
    trades: list[TradeEvent],
    window_seconds: float = 60.0,
    now_ns: int = 0,
) -> tuple[Decimal, Decimal]:
    """Return (buy_intensity, sell_intensity) over the window in units/second."""
    cutoff = now_ns - int(window_seconds * 1e9)
    buy_vol = ZERO
    sell_vol = ZERO
    for t in trades:
        if t.ts_ns < cutoff:
            continue
        if t.side == "buy":
            buy_vol += t.size
        else:
            sell_vol += t.size
    buy_intensity = buy_vol / Decimal(str(window_seconds))
    sell_intensity = sell_vol / Decimal(str(window_seconds))
    return buy_intensity, sell_intensity


def net_order_flow(trades: list[TradeEvent], window_seconds: float = 60.0, now_ns: int = 0) -> Decimal:
    """Net signed order flow: positive = net buying, negative = net selling."""
    buy_i, sell_i = trade_intensity(trades, window_seconds, now_ns)
    return buy_i - sell_i


def bid_ask_pressure_ratio(book: OrderBook, n_levels: int = 3) -> Decimal | None:
    """Bid liquidity / Ask liquidity for the top N levels."""
    bid_liq = book.available_bid_liquidity(n_levels)
    ask_liq = book.available_ask_liquidity(n_levels)
    if ask_liq == ZERO:
        return None
    return bid_liq / ask_liq


def effective_spread(last_trade_price: Decimal, mid: Decimal) -> Decimal:
    """2 * |trade_price - mid| as a measure of effective spread paid."""
    return 2 * abs(last_trade_price - mid)
