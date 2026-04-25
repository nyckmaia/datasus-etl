"""Update-check endpoint.

Compares the installed ``__version__`` against the ``VERSION`` file on the
``main`` branch of the GitHub repository. The result is cached in-process
for one day so we never hammer GitHub on dashboard polls.

Failure modes (offline, DNS, unparseable VERSION) return HTTP 200 with
``update_available: false`` and an ``error`` category — the UI is expected
to render the banner only when ``update_available`` is true, so silent
degradation is the desired behavior.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from fastapi import APIRouter
from packaging.version import InvalidVersion, Version

from datasus_etl.__version__ import __version__

router = APIRouter()

_VERSION_URL = "https://raw.githubusercontent.com/nyckmaia/datasus-etl/main/VERSION"
_RELEASE_URL_TEMPLATE = "https://github.com/nyckmaia/datasus-etl/releases/tag/v{version}"
_HTTP_TIMEOUT_SECONDS = 3.0
_CACHE_TTL_SECONDS = 24 * 3600

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


async def _fetch_latest_version(url: str) -> str:
    """Fetch raw VERSION text from GitHub. Caller handles exceptions."""
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text.strip()


@router.get("/check")
async def check_for_update() -> dict[str, Any]:
    now = time.time()
    cached = _cache.get("latest")
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    payload: dict[str, Any] = {
        "current": __version__,
        "latest": None,
        "update_available": False,
    }

    try:
        latest = await _fetch_latest_version(_VERSION_URL)
        payload["latest"] = latest
        if Version(latest) > Version(__version__):
            payload["update_available"] = True
            payload["release_url"] = _RELEASE_URL_TEMPLATE.format(version=latest)
    except httpx.HTTPError as exc:
        payload["error"] = type(exc).__name__
    except InvalidVersion as exc:
        payload["error"] = type(exc).__name__

    _cache["latest"] = (now, payload)
    return payload
