"""Threshold-based alerting — Slack webhook, log, extensible."""

from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


class AlertManager:
    """Dispatches alerts to configured channels.

    Currently supports: structured log output, Slack webhook.
    Email support is a future extension (add SMTP handler).
    """

    def __init__(self, slack_webhook: str = "", log_level: int = logging.WARNING) -> None:
        self._slack_webhook = slack_webhook or os.environ.get("SLACK_WEBHOOK", "")
        self._log_level = log_level

    async def alert(self, title: str, message: str, severity: str = "WARNING") -> None:
        """Send an alert to all configured channels."""
        full_msg = f"[{severity}] {title}: {message}"
        logger.log(self._log_level, "ALERT %s", full_msg)
        if self._slack_webhook:
            await self._send_slack(title, message, severity)

    async def circuit_breaker_alert(self, reason: str, detail: str) -> None:
        await self.alert(
            title=f"CIRCUIT BREAKER TRIGGERED: {reason}",
            message=detail,
            severity="CRITICAL",
        )

    async def drawdown_alert(self, drawdown_pct: Decimal) -> None:
        await self.alert(
            title="Drawdown Warning",
            message=f"Session drawdown reached {float(drawdown_pct)*100:.2f}%",
            severity="WARNING",
        )

    async def _send_slack(self, title: str, message: str, severity: str) -> None:
        try:
            import httpx
            colour = {"CRITICAL": "#FF0000", "WARNING": "#FFA500"}.get(severity, "#36A64F")
            payload: dict[str, Any] = {
                "attachments": [{
                    "color": colour,
                    "title": f"HFT Alert: {title}",
                    "text": message,
                    "footer": "Coinbase HFT Platform",
                }]
            }
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(self._slack_webhook, json=payload)
        except Exception as exc:
            logger.warning("Slack alert failed: %s", exc)
