"""Prometheus metrics registry for the HFT platform."""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Order metrics
# ---------------------------------------------------------------------------
ORDERS_SUBMITTED = Counter(
    "hft_orders_submitted_total",
    "Total orders submitted",
    ["product_id", "side", "order_type", "mode"],
)
ORDERS_FILLED = Counter(
    "hft_orders_filled_total",
    "Total orders filled",
    ["product_id", "side", "mode"],
)
ORDERS_CANCELLED = Counter(
    "hft_orders_cancelled_total",
    "Total orders cancelled",
    ["product_id", "mode"],
)
ORDERS_REJECTED = Counter(
    "hft_orders_rejected_total",
    "Total orders rejected (risk or exchange)",
    ["product_id", "reason"],
)

# ---------------------------------------------------------------------------
# Fill metrics
# ---------------------------------------------------------------------------
FILL_LATENCY_MS = Histogram(
    "hft_fill_latency_ms",
    "Latency from order submission to fill in milliseconds",
    ["product_id", "mode"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000],
)
FILL_SLIPPAGE_BPS = Histogram(
    "hft_fill_slippage_bps",
    "Fill slippage in basis points",
    ["product_id", "side"],
    buckets=[0, 1, 2, 5, 10, 20, 50, 100],
)

# ---------------------------------------------------------------------------
# PnL metrics
# ---------------------------------------------------------------------------
SESSION_PNL = Gauge(
    "hft_session_pnl_usd",
    "Current session net PnL in USD",
    ["mode"],
)
REALIZED_PNL = Gauge(
    "hft_realized_pnl_usd",
    "Session realized PnL in USD",
    ["mode"],
)
UNREALIZED_PNL = Gauge(
    "hft_unrealized_pnl_usd",
    "Current unrealized PnL in USD",
    ["mode"],
)
DRAWDOWN_PCT = Gauge(
    "hft_drawdown_pct",
    "Current drawdown as fraction of peak PnL",
)
FEES_PAID = Gauge(
    "hft_fees_paid_usd",
    "Total fees paid this session",
    ["mode"],
)

# ---------------------------------------------------------------------------
# Position metrics
# ---------------------------------------------------------------------------
POSITION_SIZE = Gauge(
    "hft_position_size",
    "Current position size in base currency",
    ["product_id"],
)
PORTFOLIO_VALUE = Gauge(
    "hft_portfolio_value_usd",
    "Total portfolio value in USD",
    ["mode"],
)

# ---------------------------------------------------------------------------
# Market data metrics
# ---------------------------------------------------------------------------
WS_LATENCY_MS = Gauge(
    "hft_ws_latency_ms",
    "WebSocket message latency in milliseconds",
)
WS_RECONNECTS = Counter(
    "hft_ws_reconnects_total",
    "Total WebSocket reconnection events",
)
TICK_PROCESSING_MS = Histogram(
    "hft_tick_processing_ms",
    "Time to process one market data tick (strategy + risk + order) in ms",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 25, 50, 100],
)

# ---------------------------------------------------------------------------
# Risk metrics
# ---------------------------------------------------------------------------
RISK_CHECKS_PASSED = Counter("hft_risk_checks_passed_total", "Pre-trade checks passed")
RISK_CHECKS_FAILED = Counter(
    "hft_risk_checks_failed_total",
    "Pre-trade checks failed",
    ["reason"],
)
CIRCUIT_BREAKER_TRIGGERS = Counter(
    "hft_circuit_breaker_triggers_total",
    "Circuit breaker trigger events",
    ["reason"],
)


# ---------------------------------------------------------------------------
# Slow Market Maker (SMM) metrics
# ---------------------------------------------------------------------------
SMM_QUOTES_POSTED = Counter(
    "smm_quotes_posted_total",
    "Total SMM quote rungs posted",
    ["product_id", "side"],
)
SMM_QUOTES_CANCELLED = Counter(
    "smm_quotes_cancelled_total",
    "Total SMM quote cancellations",
    ["product_id"],
)
SMM_SPREAD_BPS = Gauge(
    "smm_spread_bps",
    "Current SMM effective half-spread in bps",
    ["product_id"],
)
SMM_RESERVATION_PRICE = Gauge(
    "smm_reservation_price",
    "Current A-S reservation price",
    ["product_id"],
)
SMM_INVENTORY = Gauge(
    "smm_inventory",
    "Current SMM net inventory in base currency",
    ["product_id"],
)
SMM_REALIZED_VOL = Gauge(
    "smm_realized_vol_annualized",
    "Realized annualized volatility used in A-S model",
    ["product_id"],
)
SMM_LATENCY_P95_MS = Gauge(
    "smm_latency_p95_ms",
    "Measured RTT 95th percentile in milliseconds",
)
SMM_HALT_ACTIVE = Gauge(
    "smm_halt_active",
    "1 if volatility halt is active for product, 0 otherwise",
    ["product_id"],
)
SMM_POST_ONLY_REJECTS = Counter(
    "smm_post_only_rejects_total",
    "Total post-only orders rejected for crossing",
    ["product_id", "side"],
)
SMM_FILLS_TOTAL = Counter(
    "smm_fills_total",
    "Total SMM order fills",
    ["product_id", "side"],
)


def start_metrics_server(port: int = 9090) -> None:
    try:
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except Exception as exc:
        logger.warning("Failed to start metrics server on port %d: %s", port, exc)
