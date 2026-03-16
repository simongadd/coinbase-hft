"""Unit tests for the risk manager pre-trade checks."""

import pytest
from decimal import Decimal

from coinbase_hft.core.clock import Clock
from coinbase_hft.core.event_bus import EventBus
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.risk.circuit_breaker import CircuitBreaker
from coinbase_hft.risk.pnl_tracker import PnLTracker
from coinbase_hft.risk.position_limits import PositionLimits
from coinbase_hft.risk.risk_manager import RiskCheckFailed, RiskManager


@pytest.fixture
def risk_manager() -> RiskManager:
    positions = PositionTracker({"USD": Decimal("10000")})
    pnl_tracker = PnLTracker(positions, Decimal("10000"))
    circuit_breaker = CircuitBreaker(
        max_drawdown_pct=Decimal("0.02"),
        daily_loss_limit_usd=Decimal("200"),
        max_latency_ms=500,
        max_error_rate=5,
    )
    limits = PositionLimits(
        position_tracker=positions,
        max_position_pct=Decimal("0.25"),
        max_portfolio_exposure_pct=Decimal("0.80"),
        max_order_size_usd=Decimal("500"),
    )
    return RiskManager(
        position_tracker=positions,
        pnl_tracker=pnl_tracker,
        circuit_breaker=circuit_breaker,
        position_limits=limits,
        event_bus=EventBus(),
        clock=Clock(),
        min_order_interval_ms=0,  # disable throttle for tests
    )


@pytest.mark.asyncio
async def test_valid_order_passes(risk_manager):
    await risk_manager.check_order(
        product_id="BTC-USD",
        side="buy",
        order_type="limit",
        size=Decimal("0.001"),
        price=Decimal("50000"),
        current_prices={"BTC-USD": Decimal("50000")},
        portfolio_value_usd=Decimal("10000"),
    )


@pytest.mark.asyncio
async def test_order_too_large_rejected(risk_manager):
    with pytest.raises(RiskCheckFailed, match="max"):
        await risk_manager.check_order(
            product_id="BTC-USD",
            side="buy",
            order_type="limit",
            size=Decimal("0.1"),   # 0.1 BTC @ 50k = 5000 > max 500
            price=Decimal("50000"),
            current_prices={"BTC-USD": Decimal("50000")},
            portfolio_value_usd=Decimal("10000"),
        )


@pytest.mark.asyncio
async def test_circuit_breaker_blocks_all_orders(risk_manager):
    risk_manager._cb.trigger_manual("test")
    with pytest.raises(RiskCheckFailed, match="Circuit breaker"):
        await risk_manager.check_order(
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("0.001"),
            price=Decimal("50000"),
            current_prices={"BTC-USD": Decimal("50000")},
            portfolio_value_usd=Decimal("10000"),
        )


@pytest.mark.asyncio
async def test_spread_too_wide_rejected(risk_manager):
    risk_manager.max_spread_bps = Decimal("50")
    with pytest.raises(RiskCheckFailed, match="Spread"):
        await risk_manager.check_order(
            product_id="BTC-USD",
            side="buy",
            order_type="limit",
            size=Decimal("0.001"),
            price=Decimal("50000"),
            current_prices={"BTC-USD": Decimal("50000")},
            portfolio_value_usd=Decimal("10000"),
            book_spread_bps=Decimal("100"),
        )


@pytest.mark.asyncio
async def test_order_throttle_blocks_rapid_orders(risk_manager):
    risk_manager.min_order_interval_ms = 1000  # 1 second
    # First order should pass
    await risk_manager.check_order(
        product_id="BTC-USD", side="buy", order_type="limit",
        size=Decimal("0.001"), price=Decimal("50000"),
        current_prices={"BTC-USD": Decimal("50000")},
        portfolio_value_usd=Decimal("10000"),
    )
    # Second order immediately should fail
    with pytest.raises(RiskCheckFailed, match="throttled"):
        await risk_manager.check_order(
            product_id="BTC-USD", side="buy", order_type="limit",
            size=Decimal("0.001"), price=Decimal("50000"),
            current_prices={"BTC-USD": Decimal("50000")},
            portfolio_value_usd=Decimal("10000"),
        )
