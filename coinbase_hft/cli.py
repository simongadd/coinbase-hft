"""CLI entry point for the Coinbase HFT platform."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
app = typer.Typer(
    name="coinbase-hft",
    help="Coinbase Advanced Trade high-frequency trading platform",
    add_completion=False,
)


def _get_event_loop() -> asyncio.AbstractEventLoop:
    try:
        import uvloop
        return uvloop.new_event_loop()
    except ImportError:
        return asyncio.new_event_loop()


@app.command()
def run(
    strategy: str = typer.Option("market_making", "--strategy", "-s", help="Strategy name"),
    mode: str = typer.Option("paper", "--mode", "-m", help="Trading mode: paper | live"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to settings.yaml"),
) -> None:
    """Start the trading engine (default: paper trading mode)."""
    from coinbase_hft.config.loader import load_settings, setup_logging
    from coinbase_hft.core.clock import Clock
    from coinbase_hft.core.engine import TradingEngine
    from coinbase_hft.core.event_bus import EventBus
    from coinbase_hft.execution.fill_model import FillModel
    from coinbase_hft.execution.order_manager import OrderManager
    from coinbase_hft.execution.paper_executor import PaperExecutor
    from coinbase_hft.execution.position_tracker import PositionTracker
    from coinbase_hft.market_data.market_data_store import MarketDataStore
    from coinbase_hft.strategy.strategy_registry import get_strategy
    from coinbase_hft.utils.decimal_math import to_decimal

    settings = load_settings(config)
    settings._raw["mode"] = mode
    setup_logging(settings)

    if mode == "live":
        _confirm_live_trading()

    _print_banner(mode, strategy, settings.trading_pairs)

    # Build a minimal strategy instance — TradingEngine will inject its own
    # order_manager, data_store, and clock into it during __init__.
    strategy_cls = get_strategy(strategy)
    strategy_cfg = settings.get("strategies", strategy, default={})

    _clock = Clock()
    _bus = EventBus()
    _balances = {
        k: to_decimal(v)
        for k, v in settings.get("account", "paper_balance", default={}).items()
    } or {"USD": to_decimal("10000")}
    _positions = PositionTracker(_balances)
    _fill_model = FillModel()
    _paper_exec = PaperExecutor(_fill_model, _positions, _bus, _clock)
    _store = MarketDataStore()
    _order_mgr = OrderManager(
        mode=mode, event_bus=_bus, position_tracker=_positions,
        clock=_clock, paper_executor=_paper_exec,
    )

    strategy_instance = strategy_cls(
        product_ids=settings.trading_pairs,
        order_manager=_order_mgr,
        data_store=_store,
        clock=_clock,
        config=strategy_cfg,
    )

    # TradingEngine replaces strategy._orders/_store/_clock with its own wired-up versions.
    engine = TradingEngine(settings=settings, strategy=strategy_instance)

    loop = _get_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(engine.start())
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted — shutting down[/yellow]")
    finally:
        loop.close()


@app.command()
def backtest(
    strategy: str = typer.Option("momentum", "--strategy", "-s"),
    data_file: Path = typer.Argument(..., help="Path to historical OHLCV CSV/JSON file"),
    product_id: str = typer.Option("BTC-USD", "--pair", "-p"),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    capital: float = typer.Option(10000.0, "--capital"),
) -> None:
    """Run a backtest against historical data."""
    from coinbase_hft.backtesting.backtest_engine import BacktestEngine
    from coinbase_hft.backtesting.data_loader import load_candles_csv, load_candles_json
    from coinbase_hft.config.loader import load_settings, setup_logging
    from coinbase_hft.core.clock import SimulatedClock
    from coinbase_hft.core.event_bus import EventBus
    from coinbase_hft.execution.fill_model import FillModel
    from coinbase_hft.execution.order_manager import OrderManager
    from coinbase_hft.execution.paper_executor import PaperExecutor
    from coinbase_hft.execution.position_tracker import PositionTracker
    from coinbase_hft.market_data.market_data_store import MarketDataStore
    from coinbase_hft.strategy.strategy_registry import get_strategy
    from coinbase_hft.utils.decimal_math import to_decimal

    settings = load_settings(config)
    setup_logging(settings)

    console.print(f"[cyan]Backtesting strategy:[/cyan] {strategy}")
    console.print(f"[cyan]Data file:[/cyan] {data_file}")

    if data_file.suffix.lower() == ".json":
        candles = load_candles_json(data_file, product_id)
    else:
        candles = load_candles_csv(data_file, product_id)

    if not candles:
        console.print("[red]No candles loaded — check your data file.[/red]")
        raise typer.Exit(1)

    # Build a placeholder strategy — BacktestEngine will inject its own components.
    clock = SimulatedClock()
    bus = EventBus()
    init_bal = {"USD": to_decimal(str(capital))}
    positions = PositionTracker(init_bal)
    fill_model = FillModel(slippage_bps=5, fee_rate=to_decimal("0.006"))
    paper_exec = PaperExecutor(fill_model, positions, bus, clock)
    store = MarketDataStore()
    order_mgr = OrderManager(
        mode="paper", event_bus=bus, position_tracker=positions,
        clock=clock, paper_executor=paper_exec,
    )

    strategy_cls = get_strategy(strategy)
    strategy_cfg = settings.get("strategies", strategy, default={})
    strategy_instance = strategy_cls(
        product_ids=[product_id],
        order_manager=order_mgr,
        data_store=store,
        clock=clock,
        config=strategy_cfg,
    )

    engine = BacktestEngine(
        strategy=strategy_instance,
        product_ids=[product_id],
        initial_balance_usd=to_decimal(str(capital)),
    )

    loop = _get_event_loop()
    asyncio.set_event_loop(loop)
    report = loop.run_until_complete(engine.run(candles))
    loop.close()

    console.print(Panel(report.display(), title=f"Backtest Results: {strategy}", border_style="green"))


@app.command()
def status(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show recent sessions and PnL from the trade log."""
    from coinbase_hft.config.loader import load_settings
    from coinbase_hft.persistence.trade_log import TradeLog

    settings = load_settings(config)
    trade_log = TradeLog(settings.str("database", "path", default="data/trades.db"))

    async def _show() -> None:
        await trade_log.open()
        sessions = await trade_log.list_sessions()
        await trade_log.close()

        if not sessions:
            console.print("[yellow]No sessions found in trade log.[/yellow]")
            return

        table = Table(title="Recent Sessions", show_header=True)
        table.add_column("Session ID", style="cyan")
        table.add_column("Mode")
        table.add_column("Strategy")
        table.add_column("Pairs")
        table.add_column("Trades")
        table.add_column("PnL")

        for s in sessions[:10]:
            pnl_val = float(s.get("realized_pnl", 0))
            colour = "green" if pnl_val >= 0 else "red"
            table.add_row(
                s["session_id"][-8:],
                s["mode"].upper(),
                s["strategy"],
                s["product_ids"][:20],
                str(s.get("trade_count", 0)),
                f"[{colour}]${pnl_val:.4f}[/{colour}]",
            )
        console.print(table)

    asyncio.run(_show())


@app.command()
def test_connection(
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Validate API keys and Coinbase connectivity."""
    import os

    from dotenv import load_dotenv
    load_dotenv()

    console.print("[cyan]Testing Coinbase API connectivity...[/cyan]")

    api_key = os.environ.get("COINBASE_API_KEY", "")
    api_secret = os.environ.get("COINBASE_API_SECRET", "")

    if not api_key or not api_secret:
        console.print("[red]COINBASE_API_KEY and COINBASE_API_SECRET must be set.[/red]")
        raise typer.Exit(1)

    try:
        from coinbase.rest import RESTClient
        client = RESTClient(api_key=api_key, api_secret=api_secret)
        accounts = client.get_accounts()
        console.print("[green]✓ API connection successful[/green]")
        if hasattr(accounts, "accounts"):
            console.print(f"[cyan]Accounts found: {len(accounts.accounts)}[/cyan]")
    except Exception as exc:
        console.print(f"[red]Connection failed: {exc}[/red]")
        raise typer.Exit(1)


@app.command()
def replay(
    session: str = typer.Argument(..., help="Session ID or timestamp prefix"),
) -> None:
    """Replay a recorded session for analysis."""
    from coinbase_hft.persistence.session_recorder import SessionReplayer

    session_dir = Path("data/sessions")
    matches = list(session_dir.glob(f"{session}*.jsonl")) if session_dir.exists() else []

    if not matches:
        console.print(f"[red]No session file found matching '{session}'[/red]")
        raise typer.Exit(1)

    session_file = sorted(matches)[-1]
    replayer = SessionReplayer(session_file)
    count = replayer.count_events()
    console.print(f"[cyan]Replaying session:[/cyan] {session_file.name}")
    console.print(f"[cyan]Events:[/cyan] {count:,}")

    table = Table(title="Session Events (first 20)", show_header=True)
    table.add_column("ts_ns")
    table.add_column("type")
    table.add_column("data", max_width=60)

    for i, event in enumerate(replayer.events()):
        if i >= 20:
            break
        table.add_row(
            str(event.get("ts_ns", "")),
            event.get("type", ""),
            str(event.get("data", ""))[:60],
        )
    console.print(table)


@app.command()
def kill() -> None:
    """Send a kill signal to the running engine (writes sentinel file)."""
    kill_file = Path(".kill_switch")
    kill_file.write_text("kill")
    console.print("[red]Kill signal sent — engine will halt on next tick.[/red]")


def _confirm_live_trading() -> None:
    """Require explicit typed confirmation before live trading starts."""
    console.print(Panel(
        "[bold red]WARNING: LIVE TRADING MODE[/bold red]\n\n"
        "You are about to trade with REAL FUNDS on a funded Coinbase account.\n"
        "All orders will be executed with real money.\n\n"
        "Ensure you have reviewed all risk settings in settings.yaml before proceeding.",
        title="LIVE TRADING CONFIRMATION REQUIRED",
        border_style="red",
    ))
    confirmation = typer.prompt("Type 'CONFIRM LIVE TRADING' to proceed")
    if confirmation != "CONFIRM LIVE TRADING":
        console.print("[red]Confirmation not received — aborting.[/red]")
        raise typer.Exit(1)
    import time
    console.print("[yellow]Live trading confirmed. Starting in 3 seconds...[/yellow]")
    time.sleep(3)


def _print_banner(mode: str, strategy: str, pairs: list[str]) -> None:
    colour = "yellow" if mode == "paper" else "red"
    mode_label = "PAPER TRADING" if mode == "paper" else "*** LIVE TRADING ***"
    console.print(Panel(
        f"[bold {colour}]{mode_label}[/bold {colour}]\n\n"
        f"Strategy: {strategy}\n"
        f"Pairs:    {', '.join(pairs)}",
        title="Coinbase HFT Platform",
        border_style=colour,
    ))


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
