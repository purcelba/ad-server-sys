"""Tests against real Redis — require `make up`. Self-skip without it."""

import uuid

import pytest
import redis

from adserver.stream_features.state import SessionState


def _redis_reachable() -> bool:
    try:
        redis.Redis(host="localhost", port=6379).ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_reachable(), reason="redis not reachable — run `make up`")


@pytest.fixture
def redis_client():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    yield client
    for key in client.scan_iter("window:test_*"):
        client.delete(key)


@requires_redis
def test_record_and_count_window_counts_within_window(redis_client):
    state = SessionState(redis_client)
    user_id = f"test_{uuid.uuid4().hex[:8]}"

    c1 = state.record_and_count_window(user_id, "feat", "m1", ts_epoch=1000.0, window_seconds=600)
    c2 = state.record_and_count_window(user_id, "feat", "m2", ts_epoch=1100.0, window_seconds=600)
    assert c1 == 1
    assert c2 == 2


@requires_redis
def test_record_and_count_window_prunes_old_entries(redis_client):
    state = SessionState(redis_client)
    user_id = f"test_{uuid.uuid4().hex[:8]}"

    state.record_and_count_window(user_id, "feat", "m1", ts_epoch=1000.0, window_seconds=600)
    # far outside the 600s window relative to m1 - m1 must be pruned
    count = state.record_and_count_window(user_id, "feat", "m2", ts_epoch=1000.0 + 700, window_seconds=600)
    assert count == 1


@requires_redis
def test_record_and_count_window_isolated_per_feature(redis_client):
    state = SessionState(redis_client)
    user_id = f"test_{uuid.uuid4().hex[:8]}"

    state.record_and_count_window(user_id, "feat_a", "m1", ts_epoch=1000.0, window_seconds=600)
    count_b = state.record_and_count_window(user_id, "feat_b", "m1", ts_epoch=1000.0, window_seconds=600)
    assert count_b == 1  # feat_b's window is independent of feat_a's
