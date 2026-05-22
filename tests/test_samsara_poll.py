"""Async tests for the Samsara harsh-event poll loop (_fetch_samsara_harsh_event).

aiohttp is stubbed in conftest, so we replace the module-level _http_session with a
fake whose .get() returns an async context manager yielding a canned response, and we
patch asyncio.sleep to a no-op so the 20s waits are instant.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import utils.webhook_handler as wh


# ── fakes ─────────────────────────────────────────────────────────────────────────

def _resp(status=200, json_data=None):
    r = MagicMock()
    r.status = status
    r.json = AsyncMock(return_value=json_data or {})
    r.text = AsyncMock(return_value="")
    return r

def _async_cm(resp):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm

def _session(responses):
    """Fake aiohttp session. responses is a list of canned responses, one per .get()
    call; the last one repeats for any further calls."""
    session = MagicMock()
    cms = [_async_cm(r) for r in responses]

    def _get(*args, **kwargs):
        return cms[min(session.get.call_count - 1, len(cms) - 1)]

    session.get = MagicMock(side_effect=_get)
    return session


@pytest.fixture(autouse=True)
def _no_sleep_and_key(monkeypatch):
    # Kill the 20s waits and give the header an API key (config stub has none).
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(wh.config, "SAMSARA_API_KEY", "test", raising=False)


def _install(monkeypatch, responses):
    session = _session(responses)
    monkeypatch.setattr(wh, "_http_session", session)
    return session


# ── tests ───────────────────────────────────────────────────────────────────────

async def test_both_urls_returns_immediately(monkeypatch):
    data = {"harshEventType": "Harsh Braking",
            "downloadForwardVideoUrl": "fwd", "downloadInwardVideoUrl": "in"}
    session = _install(monkeypatch, [_resp(json_data=data)])
    result = await wh._fetch_samsara_harsh_event("veh1", 123)
    assert result == data
    assert session.get.call_count == 1  # no extra polling once both clips are ready

async def test_inward_only_short_circuits_on_inward(monkeypatch):
    # "Mobile Usage" → cell_phone (inward-only): the inward clip alone is enough.
    data = {"harshEventType": "Mobile Usage", "downloadInwardVideoUrl": "in"}
    session = _install(monkeypatch, [_resp(json_data=data)])
    result = await wh._fetch_samsara_harsh_event("veh1", 123)
    assert result == data
    assert session.get.call_count == 1  # didn't wait for a forward URL that never comes

async def test_crash_extends_window_to_15_and_never_early_bails(monkeypatch):
    # Crash with no clips ever: must poll the full extended window, not bail at 3.
    data = {"harshEventType": "Crash"}
    session = _install(monkeypatch, [_resp(json_data=data)])
    result = await wh._fetch_samsara_harsh_event("veh1", 123)
    assert session.get.call_count == 15
    assert result == data  # falls back to last_data after the window

async def test_http_error_gives_up_after_one_call(monkeypatch):
    session = _install(monkeypatch, [_resp(status=500)])
    result = await wh._fetch_samsara_harsh_event("veh1", 123)
    assert result is None
    assert session.get.call_count == 1

async def test_standard_event_no_media_bails_at_three(monkeypatch):
    # Forward-facing type (hard_brake) with no clips ever → bail at attempt 3.
    data = {"harshEventType": "Harsh Braking"}
    session = _install(monkeypatch, [_resp(json_data=data)])
    result = await wh._fetch_samsara_harsh_event("veh1", 123)
    assert session.get.call_count == 3
    assert result == data  # sends with no media rather than losing the alert
