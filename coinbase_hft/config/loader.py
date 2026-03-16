"""Configuration loader — merges YAML config with environment variable overrides."""

from __future__ import annotations

import logging
import logging.config
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).parent / "settings.yaml"
_DEFAULT_LOGGING_PATH = Path(__file__).parent / "logging.yaml"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base, returning a new dict."""
    result = base.copy()
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


class Settings:
    """Typed wrapper around the raw YAML config dict."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self._raw = raw

    def get(self, *keys: str, default: Any = None) -> Any:
        node: Any = self._raw
        for key in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(key, default)
            if node is None:
                return default
        return node

    def decimal(self, *keys: str, default: str = "0") -> Decimal:
        val = self.get(*keys, default=default)
        return Decimal(str(val))

    def int(self, *keys: str, default: int = 0) -> int:
        return int(self.get(*keys, default=default))

    def str(self, *keys: str, default: str = "") -> str:
        return str(self.get(*keys, default=default))

    def bool(self, *keys: str, default: bool = False) -> bool:
        val = self.get(*keys, default=default)
        if isinstance(val, bool):
            return val
        return str(val).lower() in ("true", "1", "yes")

    def list(self, *keys: str, default: list[Any] | None = None) -> list[Any]:
        val = self.get(*keys, default=default or [])
        return val if isinstance(val, list) else [val]

    @property
    def mode(self) -> str:
        return os.environ.get("TRADING_MODE", self.str("mode", default="paper"))

    @property
    def is_paper(self) -> bool:
        return self.mode == "paper"

    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    @property
    def trading_pairs(self) -> list[str]:
        return self.list("trading", "pairs", default=["BTC-USD"])

    @property
    def raw(self) -> dict[str, Any]:
        return self._raw


def load_settings(config_path: Path | str | None = None) -> Settings:
    """Load and return a Settings object from YAML + env overrides."""
    load_dotenv()

    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    # Env-variable overrides for the most critical settings
    env_overrides: dict[str, Any] = {}
    if mode := os.environ.get("TRADING_MODE"):
        env_overrides["mode"] = mode

    raw = _deep_merge(raw, env_overrides)
    return Settings(raw)


def setup_logging(settings: Settings, log_config_path: Path | str | None = None) -> None:
    """Configure logging from the logging YAML, creating log dirs as needed."""
    path = Path(log_config_path) if log_config_path else _DEFAULT_LOGGING_PATH

    log_dir = Path(settings.str("logging", "file", default="logs/hft.log")).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path(settings.str("database", "path", default="data/trades.db")).parent
    data_dir.mkdir(parents=True, exist_ok=True)

    if path.exists():
        with path.open() as f:
            log_cfg = yaml.safe_load(f)
        # Ensure log directories for file handlers exist
        for handler in log_cfg.get("handlers", {}).values():
            if filename := handler.get("filename"):
                Path(filename).parent.mkdir(parents=True, exist_ok=True)
        logging.config.dictConfig(log_cfg)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
        )
