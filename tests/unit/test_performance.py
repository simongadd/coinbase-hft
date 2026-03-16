"""Unit tests for backtesting performance metrics."""

import pytest
from decimal import Decimal

from coinbase_hft.backtesting.performance import compute_performance
from coinbase_hft.utils.decimal_math import ZERO


def _d(v: float) -> Decimal:
    return Decimal(str(v))


def test_empty_trades():
    report = compute_performance([])
    assert report.total_trades == 0
    assert report.win_rate == ZERO
    assert report.total_pnl == ZERO


def test_all_winning_trades():
    pnls = [_d(10), _d(20), _d(5)]
    report = compute_performance(pnls)
    assert report.total_trades == 3
    assert report.winning_trades == 3
    assert report.losing_trades == 0
    assert report.win_rate == Decimal("1")
    assert report.total_pnl == _d(35)


def test_all_losing_trades():
    pnls = [_d(-10), _d(-20)]
    report = compute_performance(pnls)
    assert report.winning_trades == 0
    assert report.losing_trades == 2
    assert report.win_rate == ZERO


def test_mixed_trades():
    pnls = [_d(10), _d(-5), _d(15), _d(-3)]
    report = compute_performance(pnls)
    assert report.winning_trades == 2
    assert report.losing_trades == 2
    assert report.win_rate == Decimal("0.5")
    assert report.total_pnl == _d(17)
    assert report.gross_profit == _d(25)
    assert report.gross_loss == _d(-8)
    assert report.profit_factor == _d(25) / _d(8)


def test_max_drawdown():
    # Equity curve: 10000, 10010, 10020, 10000, 9990 (drawdown of 30)
    pnls = [_d(10), _d(10), _d(-20), _d(-10)]
    report = compute_performance(pnls, initial_capital=_d(10000))
    assert report.max_drawdown == _d(30)


def test_sharpe_ratio_computed():
    pnls = [_d(5)] * 30  # constant positive returns
    report = compute_performance(pnls)
    # With constant returns, std=0 so sharpe is None
    assert report.sharpe_ratio is None  # or a value if std is non-zero


def test_report_display_is_string():
    pnls = [_d(10), _d(-5), _d(8)]
    report = compute_performance(pnls)
    display = report.display()
    assert isinstance(display, str)
    assert "Win Rate" in display
    assert "PnL" in display
