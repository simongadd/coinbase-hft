"""Shared pytest fixtures for HFT test suite."""

from __future__ import annotations

import pytest
from decimal import Decimal

from coinbase_hft.core.clock import SimulatedClock
from coinbase_hft.core.event_bus import EventBus
from coinbase_hft.execution.fill_model import FillModel
from coinbase_hft.execution.order_manager import OrderManager
from coinbase_hft.execution.paper_executor import PaperExecutor
from coinbase_hft.execution.position_tracker import PositionTracker
from coinbase_hft.market_data.market_data_store import MarketDataStore


@pytest.fixture
def clock():
    return SimulatedClock(start_ns=1_000_000_000_000)


@pytest.fixture
def event_bus():
    return EventBus()


@pytest.fixture
def position_tracker():
    return PositionTracker({
        "USD": Decimal("10000"),
        "GBP": Decimal("10000"),
        "EUR": Decimal("10000"),
        "BTC": Decimal("0"),
        "ETH": Decimal("0"),
    })


@pytest.fixture
def fill_model():
    return FillModel(
        slippage_bps=0,
        fill_probability=Decimal("1.0"),
        fee_rate=Decimal("0.0006"),
        queue_position_factor=Decimal("0.0"),
    )


@pytest.fixture
def paper_executor(fill_model, position_tracker, event_bus, clock):
    return PaperExecutor(fill_model, position_tracker, event_bus, clock)


@pytest.fixture
def order_manager(event_bus, position_tracker, clock, paper_executor):
    return OrderManager(
        mode="paper",
        event_bus=event_bus,
        position_tracker=position_tracker,
        clock=clock,
        paper_executor=paper_executor,
    )


@pytest.fixture
def market_data_store():
    return MarketDataStore()
