"""Strategy performance metrics: Sharpe, max drawdown, win rate, profit factor."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import NamedTuple

from coinbase_hft.utils.decimal_math import ZERO


class PerformanceReport(NamedTuple):
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: Decimal           # fraction 0–1
    total_pnl: Decimal
    gross_profit: Decimal
    gross_loss: Decimal
    profit_factor: Decimal      # gross_profit / abs(gross_loss)
    avg_win: Decimal
    avg_loss: Decimal
    max_drawdown: Decimal       # absolute
    max_drawdown_pct: Decimal   # fraction
    sharpe_ratio: Decimal | None
    annualised_return: Decimal | None

    def display(self) -> str:
        pf = f"{self.profit_factor:.2f}" if self.profit_factor != ZERO else "N/A"
        sr = f"{self.sharpe_ratio:.2f}" if self.sharpe_ratio else "N/A"
        return (
            f"{'─'*50}\n"
            f"  Trades:        {self.total_trades} "
            f"(W:{self.winning_trades} L:{self.losing_trades})\n"
            f"  Win Rate:      {float(self.win_rate)*100:.1f}%\n"
            f"  Total PnL:     ${self.total_pnl:.4f}\n"
            f"  Profit Factor: {pf}\n"
            f"  Max Drawdown:  ${self.max_drawdown:.4f} ({float(self.max_drawdown_pct)*100:.2f}%)\n"
            f"  Sharpe Ratio:  {sr}\n"
            f"{'─'*50}"
        )


def compute_performance(
    trade_pnls: list[Decimal],
    initial_capital: Decimal = Decimal("10000"),
    periods_per_year: int = 252,
) -> PerformanceReport:
    """Compute full performance statistics from a list of per-trade PnL values."""
    if not trade_pnls:
        return PerformanceReport(
            total_trades=0, winning_trades=0, losing_trades=0,
            win_rate=ZERO, total_pnl=ZERO, gross_profit=ZERO, gross_loss=ZERO,
            profit_factor=ZERO, avg_win=ZERO, avg_loss=ZERO,
            max_drawdown=ZERO, max_drawdown_pct=ZERO,
            sharpe_ratio=None, annualised_return=None,
        )

    wins = [p for p in trade_pnls if p > ZERO]
    losses = [p for p in trade_pnls if p <= ZERO]

    total_pnl = sum(trade_pnls)
    gross_profit = sum(wins) if wins else ZERO
    gross_loss = sum(losses) if losses else ZERO
    win_rate = Decimal(len(wins)) / Decimal(len(trade_pnls))
    profit_factor = gross_profit / abs(gross_loss) if gross_loss != ZERO else ZERO
    avg_win = gross_profit / Decimal(len(wins)) if wins else ZERO
    avg_loss = gross_loss / Decimal(len(losses)) if losses else ZERO

    # Drawdown
    equity = initial_capital
    peak = equity
    max_dd = ZERO
    for pnl in trade_pnls:
        equity += pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
    max_dd_pct = max_dd / initial_capital if initial_capital > ZERO else ZERO

    # Sharpe ratio (annualised) — uses daily PnL series approximation
    if len(trade_pnls) >= 2:
        mean = total_pnl / Decimal(len(trade_pnls))
        variance = sum((p - mean) ** 2 for p in trade_pnls) / Decimal(len(trade_pnls))
        std = variance.sqrt() if variance > ZERO else ZERO
        if std > ZERO:
            sharpe = (mean / std) * Decimal(str(math.sqrt(periods_per_year)))
        else:
            sharpe = None
    else:
        sharpe = None

    ann_return = (total_pnl / initial_capital) * Decimal(periods_per_year) / Decimal(len(trade_pnls)) if trade_pnls else None

    return PerformanceReport(
        total_trades=len(trade_pnls),
        winning_trades=len(wins),
        losing_trades=len(losses),
        win_rate=win_rate,
        total_pnl=total_pnl,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        avg_win=avg_win,
        avg_loss=avg_loss,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        sharpe_ratio=sharpe,
        annualised_return=ann_return,
    )
