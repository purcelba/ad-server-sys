"""Requires real Redis (make up) since this handler uses SessionState."""

import datetime as dt
import uuid

import pytest
import redis

from adserver.common.events import SessionEvent
from adserver.stream_features.handlers.app_screen_view import AppScreenViewHandler
from adserver.stream_features.state import SessionState


def _redis_reachable() -> bool:
    try:
        redis.Redis(host="localhost", port=6379).ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_reachable(), reason="redis not reachable — run `make up`")


def _event(user_id, event_id, ts):
    return SessionEvent(
        event_id=event_id,
        event_type="app_screen_view",
        user_id=user_id,
        session_id="s_1",
        ts=ts,
        payload={"screen": "home"},
    )


@requires_redis
def test_app_screen_view_handler_counts_increment():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    state = SessionState(client)
    handler = AppScreenViewHandler()
    user_id = f"test_{uuid.uuid4().hex[:8]}"
    base = dt.datetime(2026, 7, 18, 12, 0, 0)

    writes1 = handler.update(_event(user_id, "e1", base), state)
    assert writes1["user_screens_viewed_10min"] == 1
    assert writes1["user_session_active"] is True

    writes2 = handler.update(_event(user_id, "e2", base + dt.timedelta(minutes=2)), state)
    assert writes2["user_screens_viewed_10min"] == 2

    for key in client.scan_iter(f"window:{user_id}:*"):
        client.delete(key)


@requires_redis
def test_app_screen_view_handler_window_expires_old_views():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    state = SessionState(client)
    handler = AppScreenViewHandler()
    user_id = f"test_{uuid.uuid4().hex[:8]}"
    base = dt.datetime(2026, 7, 18, 12, 0, 0)

    handler.update(_event(user_id, "e1", base), state)
    # 11 minutes later - e1 must have fallen out of the 10-minute window
    writes = handler.update(_event(user_id, "e2", base + dt.timedelta(minutes=11)), state)
    assert writes["user_screens_viewed_10min"] == 1

    for key in client.scan_iter(f"window:{user_id}:*"):
        client.delete(key)
