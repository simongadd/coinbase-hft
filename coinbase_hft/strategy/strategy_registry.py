"""Auto-discovery and loading of strategy classes."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from coinbase_hft.strategy.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, type["BaseStrategy"]] = {}


def register(cls: type["BaseStrategy"]) -> type["BaseStrategy"]:
    """Class decorator: register a strategy class by its `name` attribute."""
    _REGISTRY[cls.name] = cls
    logger.debug("Registered strategy: %s", cls.name)
    return cls


def discover() -> None:
    """Import all modules in coinbase_hft.strategy.examples to trigger @register."""
    import coinbase_hft.strategy.examples as pkg
    for _finder, modname, _is_pkg in pkgutil.walk_packages(
        path=pkg.__path__,
        prefix=pkg.__name__ + ".",
    ):
        try:
            importlib.import_module(modname)
        except Exception as exc:
            logger.warning("Failed to import strategy module %s: %s", modname, exc)


def get_strategy(name: str) -> type["BaseStrategy"]:
    if not _REGISTRY:
        discover()
    try:
        return _REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available: {available}") from None


def list_strategies() -> list[str]:
    if not _REGISTRY:
        discover()
    return sorted(_REGISTRY.keys())
