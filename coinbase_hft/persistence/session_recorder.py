"""Session recorder — captures market data + decisions for post-analysis and replay."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SessionRecorder:
    """Streams market data events and order decisions to a JSONL file.

    File format: one JSON object per line, fields: ts_ns, type, data.
    The replay engine reads this file to reconstruct the session.
    """

    def __init__(self, session_id: str, output_dir: str | Path = "data/sessions") -> None:
        self.session_id = session_id
        self._dir = Path(output_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / f"{session_id}.jsonl"
        self._file = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self._writer_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._file = open(self._path, "w", buffering=1)
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info("Session recorder started: %s", self._path)

    async def stop(self) -> None:
        if self._writer_task:
            await self._queue.put(None)  # sentinel
            await self._writer_task
        if self._file:
            self._file.close()
        logger.info("Session recorder stopped")

    async def record(self, ts_ns: int, event_type: str, data: dict) -> None:
        """Non-blocking enqueue of an event."""
        try:
            self._queue.put_nowait({"ts_ns": ts_ns, "type": event_type, "data": data})
        except asyncio.QueueFull:
            logger.warning("Session recorder queue full — dropping event %s", event_type)

    async def _writer_loop(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                break
            if self._file:
                self._file.write(json.dumps(item, default=str) + "\n")


class SessionReplayer:
    """Reads a recorded session JSONL file for post-analysis."""

    def __init__(self, session_file: str | Path) -> None:
        self._path = Path(session_file)

    def events(self):
        """Yield events in chronological order."""
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def count_events(self) -> int:
        count = 0
        with open(self._path) as f:
            for _ in f:
                count += 1
        return count
