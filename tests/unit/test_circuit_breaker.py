"""Unit tests for circuit breaker logic."""

import pytest
from decimal import Decimal

from coinbase_hft.risk.circuit_breaker import CircuitBreaker, CircuitBreakerReason


@pytest.fixture
def cb() -> CircuitBreaker:
    return CircuitBreaker(
        max_drawdown_pct=Decimal("0.02"),
        daily_loss_limit_usd=Decimal("200"),
        max_latency_ms=500,
        max_error_rate=5,
        error_window_seconds=60,
    )


def test_not_triggered_initially(cb):
    assert not cb.is_triggered


def test_drawdown_triggers_at_exact_threshold(cb):
    triggered = cb.check_drawdown(Decimal("-0.02"))
    assert triggered
    assert cb.is_triggered
    assert cb.trigger_event.reason == CircuitBreakerReason.DRAWDOWN


def test_drawdown_does_not_trigger_below_threshold(cb):
    triggered = cb.check_drawdown(Decimal("-0.019"))
    assert not triggered
    assert not cb.is_triggered


def test_daily_loss_triggers(cb):
    triggered = cb.check_daily_loss(Decimal("-200.01"))
    assert triggered
    assert cb.is_triggered
    assert cb.trigger_event.reason == CircuitBreakerReason.DAILY_LOSS


def test_daily_loss_exact_limit_triggers(cb):
    triggered = cb.check_daily_loss(Decimal("-200"))
    assert triggered


def test_daily_loss_under_limit_ok(cb):
    triggered = cb.check_daily_loss(Decimal("-199.99"))
    assert not triggered


def test_latency_triggers(cb):
    triggered = cb.check_latency(501.0)
    assert triggered
    assert cb.trigger_event.reason == CircuitBreakerReason.LATENCY


def test_latency_at_limit_ok(cb):
    triggered = cb.check_latency(500.0)
    assert not triggered


def test_error_rate_triggers_after_threshold(cb):
    for i in range(5):
        cb.record_error()
    assert not cb.is_triggered  # 5 errors, threshold is >5
    triggered = cb.record_error()
    assert triggered
    assert cb.is_triggered


def test_manual_trigger(cb):
    cb.trigger_manual("test")
    assert cb.is_triggered
    assert cb.trigger_event.reason == CircuitBreakerReason.MANUAL


def test_double_trigger_is_idempotent(cb):
    cb.check_drawdown(Decimal("-0.05"))
    first_event = cb.trigger_event
    cb.check_drawdown(Decimal("-0.10"))
    # Should still be the first event
    assert cb.trigger_event is first_event


def test_reset_clears_state(cb):
    cb.check_drawdown(Decimal("-0.05"))
    assert cb.is_triggered
    cb.reset()
    assert not cb.is_triggered
    assert cb.trigger_event is None
