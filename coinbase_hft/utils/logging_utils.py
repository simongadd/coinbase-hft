"""Logging helpers: JSON formatter and mode-tag filter."""

from __future__ import annotations

import json
import logging
from typing import Any


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        # Carry through any extra fields attached to the record
        for key, val in record.__dict__.items():
            if key not in (
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "id", "levelname", "levelno", "lineno", "module",
                "msecs", "message", "msg", "name", "pathname", "process",
                "processName", "relativeCreated", "stack_info", "thread",
                "threadName",
            ):
                data[key] = val
        return json.dumps(data)


class ModeTagFilter(logging.Filter):
    """Inject the current trading mode into every log record."""

    def __init__(self, mode: str = "paper") -> None:
        super().__init__()
        self.mode = mode

    def filter(self, record: logging.LogRecord) -> bool:
        record.mode = self.mode  # type: ignore[attr-defined]
        return True
