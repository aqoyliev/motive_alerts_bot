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


# ── on_first hook ─────────────────────────────────────────────────────────────────

async def test_on_first_invoked_once_with_first_typed_response(monkeypatch):
    data = {"harshEventType": "Harsh Braking",
            "downloadForwardVideoUrl": "f", "downloadInwardVideoUrl": "i"}
    _install(monkeypatch, [_resp(json_data=data)])
    hook = AsyncMock()
    await wh._fetch_samsara_harsh_event("veh1", 123, on_first=hook)
    hook.assert_awaited_once()
    assert hook.await_args.args[0] == data

async def test_on_first_not_invoked_on_obstructed(monkeypatch):
    _install(monkeypatch, [_resp(json_data={"harshEventType": "Obstructed Camera"})])
    hook = AsyncMock()
    result = await wh._fetch_samsara_harsh_event("veh1", 123, on_first=hook)
    assert result is None
    hook.assert_not_awaited()  # obstructed events are dropped before the hook

async def test_on_first_invoked_once_across_crash_window(monkeypatch):
    # Crash with no media ever: polls the full 15-attempt window but the hook (which
    # persists + fires the pending alert) must fire exactly once.
    session = _install(monkeypatch, [_resp(json_data={"harshEventType": "Crash"})])
    hook = AsyncMock()
    await wh._fetch_samsara_harsh_event("veh1", 123, on_first=hook)
    assert session.get.call_count == 15
    hook.assert_awaited_once()


# ── _handle_event: early persist + crash pending ──────────────────────────────────

def _patch_handle_event_deps(monkeypatch, groups, dms):
    save = AsyncMock()
    sent_text = []

    async def _fake_send_text(bot, chat_id, text, retries, delay):
        sent_text.append((chat_id, text))

    monkeypatch.setattr(wh, "save_violation", save)
    monkeypatch.setattr(wh, "get_groups_for_event", AsyncMock(return_value=groups))
    monkeypatch.setattr(wh, "get_subscribed_admins", AsyncMock(return_value=dms))
    monkeypatch.setattr(wh, "_send_text", _fake_send_text)
    monkeypatch.setattr(wh, "_download", AsyncMock(return_value=b"vid"))
    send_full = AsyncMock(return_value=None)
    monkeypatch.setattr(wh, "_send_with_retry", send_full)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(wh.config, "SAMSARA_API_KEY", "test", raising=False)
    return save, sent_text, send_full


def _samsara_event():
    return {
        "id": "uuid-1", "type": "harsh_event", "_source": "samsara",
        "_samsara_vehicle_id": "v1", "_samsara_timestamp_ms": 123,
        "vehicle": {"number": "528609"}, "driver": {"name": ""},
        "start_time": "2026-04-29T09:15:31Z",
    }

async def test_handle_event_crash_persists_early_and_sends_pending(monkeypatch):
    save, sent_text, send_full = _patch_handle_event_deps(monkeypatch, groups=[111], dms=[222])
    # attempt 1: Crash, no media (fires hook → early save + pending)
    # attempt 2: Crash, both URLs (full card follows)
    r1 = _resp(json_data={"harshEventType": "Crash"})
    r2 = _resp(json_data={"harshEventType": "Crash", "downloadForwardVideoUrl": "f",
                          "downloadInwardVideoUrl": "i", "location": {"address": "I-95 N"}})
    monkeypatch.setattr(wh, "_http_session", _session([r1, r2]))

    await wh._handle_event(MagicMock(), _samsara_event(), company_slug="hf")

    # persisted exactly once, as a crash, before the poll finished
    save.assert_awaited_once()
    assert save.await_args.kwargs["event_type"] == "crash"
    # pending alert went to both crash targets
    assert {cid for cid, _ in sent_text} == {111, 222}
    assert all("Crash detected — video pending" in t for _, t in sent_text)
    # full card delivered to both targets afterward
    assert send_full.await_count == 2

async def test_handle_event_noncrash_persists_early_without_pending(monkeypatch):
    save, sent_text, send_full = _patch_handle_event_deps(monkeypatch, groups=[111], dms=[])
    data = {"harshEventType": "Harsh Braking", "downloadForwardVideoUrl": "f",
            "downloadInwardVideoUrl": "i"}
    monkeypatch.setattr(wh, "_http_session", _session([_resp(json_data=data)]))

    await wh._handle_event(MagicMock(), _samsara_event(), company_slug="hf")

    save.assert_awaited_once()
    assert save.await_args.kwargs["event_type"] == "hard_brake"
    assert sent_text == []          # non-crash → no pending alert
    assert send_full.await_count == 1

async def test_handle_event_crash_no_media_skips_dm_followup(monkeypatch):
    save, sent_text, send_full = _patch_handle_event_deps(monkeypatch, groups=[111], dms=[222])
    # Crash that never produces a clip → polls the full window, no media.
    monkeypatch.setattr(wh, "_http_session", _session([_resp(json_data={"harshEventType": "Crash"})]))

    await wh._handle_event(MagicMock(), _samsara_event(), company_slug="hf")

    # Instant pending alert still reached both crash targets.
    assert {cid for cid, _ in sent_text} == {111, 222}
    # Media-less follow-up: group gets it (adds location), DM is skipped.
    called_chats = {c.args[1] for c in send_full.await_args_list}
    assert called_chats == {111}
    assert send_full.await_count == 1
