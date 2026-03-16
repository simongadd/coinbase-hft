"""Fetch per-product metadata from Coinbase REST API."""

from __future__ import annotations

import logging
import os
import secrets
import time
from dataclasses import dataclass
from decimal import Decimal

from coinbase_hft.utils.decimal_math import to_decimal

logger = logging.getLogger(__name__)

# Hardcoded fallback defaults keyed by product_id prefix
_FALLBACK_META: dict[str, dict] = {
    "BTC": {"quote_increment": "0.01", "base_increment": "0.00000001", "base_min_size": "0.0001", "base_max_size": "1000"},
    "ETH": {"quote_increment": "0.01", "base_increment": "0.00000001", "base_min_size": "0.001", "base_max_size": "10000"},
    "DEFAULT": {"quote_increment": "0.01", "base_increment": "0.00000001", "base_min_size": "0.001", "base_max_size": "10000"},
}


@dataclass
class ProductMeta:
    product_id: str
    quote_increment: Decimal
    base_increment: Decimal
    base_min_size: Decimal
    base_max_size: Decimal


def _fallback_meta(product_id: str) -> ProductMeta:
    base = product_id.split("-")[0]
    d = _FALLBACK_META.get(base, _FALLBACK_META["DEFAULT"])
    return ProductMeta(
        product_id=product_id,
        quote_increment=to_decimal(d["quote_increment"]),
        base_increment=to_decimal(d["base_increment"]),
        base_min_size=to_decimal(d["base_min_size"]),
        base_max_size=to_decimal(d["base_max_size"]),
    )


async def fetch_product_metas(product_ids: list[str]) -> dict[str, ProductMeta]:
    """Fetch per-product tick-size metadata from Coinbase REST API.

    Falls back to hardcoded defaults on error.
    """
    try:
        import httpx
        import jwt
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        api_key = os.environ.get("COINBASE_API_KEY", "")
        api_secret = os.environ.get("COINBASE_API_SECRET", "")

        if not api_key or not api_secret:
            logger.warning("No API credentials — using fallback product metadata")
            return {pid: _fallback_meta(pid) for pid in product_ids}

        private_key = load_pem_private_key(api_secret.encode("utf-8"), password=None)
        now = int(time.time())
        token = jwt.encode(
            {
                "sub": api_key,
                "iss": "coinbase-cloud",
                "nbf": now,
                "exp": now + 120,
                "aud": ["retail_rest_api_proxy"],
            },
            private_key,
            algorithm="ES256",
            headers={"kid": api_key, "nonce": secrets.token_hex()},
        )

        result: dict[str, ProductMeta] = {}
        async with httpx.AsyncClient(timeout=10.0) as client:
            for pid in product_ids:
                try:
                    resp = await client.get(
                        f"https://api.coinbase.com/api/v3/brokerage/products/{pid}",
                        headers={"Authorization": f"Bearer {token}"},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    result[pid] = ProductMeta(
                        product_id=pid,
                        quote_increment=to_decimal(data.get("quote_increment", "0.01")),
                        base_increment=to_decimal(data.get("base_increment", "0.00000001")),
                        base_min_size=to_decimal(data.get("base_min_size", "0.001")),
                        base_max_size=to_decimal(data.get("base_max_size", "10000")),
                    )
                except Exception as exc:
                    logger.warning("Failed to fetch metadata for %s: %s — using fallback", pid, exc)
                    result[pid] = _fallback_meta(pid)
        return result

    except ImportError:
        logger.warning("httpx not installed — using fallback product metadata")
        return {pid: _fallback_meta(pid) for pid in product_ids}
    except Exception as exc:
        logger.warning("product_info fetch failed: %s — using fallback", exc)
        return {pid: _fallback_meta(pid) for pid in product_ids}
