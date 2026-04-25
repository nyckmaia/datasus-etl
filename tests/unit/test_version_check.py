"""Tests for the /api/version/check endpoint.

These tests stub the network layer so they run offline. They exercise:

- update detection when GitHub reports a higher version
- no-update when GitHub reports an equal or lower version
- silent failure on network errors / malformed VERSION
- cache TTL: a successful response is reused within the window
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import httpx
import pytest

from datasus_etl.web.routes import version as version_route


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    """Each test starts with a fresh cache so order doesn't matter."""
    version_route._cache.clear()


def _stub_fetch(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    async def _fake(url: str) -> str:  # noqa: ARG001
        return value
    monkeypatch.setattr(version_route, "_fetch_latest_version", _fake)


def _stub_raise(monkeypatch: pytest.MonkeyPatch, exc: BaseException) -> None:
    async def _fake(url: str) -> str:  # noqa: ARG001
        raise exc
    monkeypatch.setattr(version_route, "_fetch_latest_version", _fake)


def _run(coro: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coro)


def test_update_available_when_remote_is_newer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    _stub_fetch(monkeypatch, "0.2.0")

    payload = _run(version_route.check_for_update())

    assert payload["current"] == "0.1.9"
    assert payload["latest"] == "0.2.0"
    assert payload["update_available"] is True
    assert payload["release_url"].endswith("/v0.2.0")


def test_no_update_when_remote_equal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    _stub_fetch(monkeypatch, "0.1.9")

    payload = _run(version_route.check_for_update())

    assert payload["latest"] == "0.1.9"
    assert payload["update_available"] is False
    assert "release_url" not in payload


def test_no_update_when_remote_older(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.2.0")
    _stub_fetch(monkeypatch, "0.1.9")

    payload = _run(version_route.check_for_update())

    assert payload["update_available"] is False


def test_silent_failure_on_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    _stub_raise(monkeypatch, httpx.ConnectError("offline"))

    payload = _run(version_route.check_for_update())

    assert payload["update_available"] is False
    assert payload["latest"] is None
    assert payload["error"] == "ConnectError"


def test_silent_failure_on_malformed_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    _stub_fetch(monkeypatch, "not-a-version")

    payload = _run(version_route.check_for_update())

    assert payload["update_available"] is False
    assert payload["error"] == "InvalidVersion"


def test_cache_reuse_within_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    calls = {"count": 0}

    async def _counting(url: str) -> str:  # noqa: ARG001
        calls["count"] += 1
        return "0.2.0"
    monkeypatch.setattr(version_route, "_fetch_latest_version", _counting)

    first = _run(version_route.check_for_update())
    second = _run(version_route.check_for_update())

    assert first == second
    assert calls["count"] == 1


def test_cache_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version_route, "__version__", "0.1.9")
    _stub_fetch(monkeypatch, "0.2.0")

    _run(version_route.check_for_update())
    cached_at, payload = version_route._cache["latest"]
    expired_at = cached_at - version_route._CACHE_TTL_SECONDS - 1
    version_route._cache["latest"] = (expired_at, payload)

    calls = {"count": 0}

    async def _counting(url: str) -> str:  # noqa: ARG001
        calls["count"] += 1
        return "0.3.0"
    monkeypatch.setattr(version_route, "_fetch_latest_version", _counting)

    refreshed = _run(version_route.check_for_update())

    assert refreshed["latest"] == "0.3.0"
    assert calls["count"] == 1
