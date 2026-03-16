"""Liveness and readiness health probes served over HTTP."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

logger = logging.getLogger(__name__)

_health_state: dict[str, Any] = {
    "ready": False,
    "live": True,
    "ws_connected": False,
    "last_tick_ts": 0.0,
    "circuit_breaker": False,
}


def set_ready(ready: bool) -> None:
    _health_state["ready"] = ready


def set_ws_connected(connected: bool) -> None:
    _health_state["ws_connected"] = connected


def record_tick() -> None:
    _health_state["last_tick_ts"] = time.monotonic()


def set_circuit_breaker(triggered: bool) -> None:
    _health_state["circuit_breaker"] = triggered


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:
        pass  # suppress default HTTP logs

    def do_GET(self) -> None:
        if self.path == "/health/live":
            self._respond(200, {"status": "ok", "live": True})
        elif self.path == "/health/ready":
            ready = (
                _health_state["ready"]
                and _health_state["ws_connected"]
                and not _health_state["circuit_breaker"]
            )
            code = 200 if ready else 503
            self._respond(code, {**_health_state, "ready": ready})
        elif self.path == "/health":
            self._respond(200, _health_state)
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def start_health_server(port: int = 8080) -> None:
    def _run() -> None:
        server = HTTPServer(("0.0.0.0", port), _Handler)
        logger.info("Health check server started on port %d", port)
        server.serve_forever()

    import threading
    t = threading.Thread(target=_run, daemon=True)
    t.start()
