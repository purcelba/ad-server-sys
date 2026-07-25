"""process_event() is directly callable without a running Kafka consumer
- these tests exercise dispatch, unknown-event handling, and Redis
writes against real Redis (make up), no Kafka needed."""

import datetime as dt
import json
import uuid

import pytest
import redis

from adserver.common.events import SessionEvent
from adserver.stream_features.consumer import discover_handlers, process_event
from adserver.stream_features.metrics import Metrics
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


@pytest.fixture
def user_id():
    return f"test_{uuid.uuid4().hex[:8]}"


def _event_json(user_id, event_type, payload, ts=None):
    event = SessionEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        user_id=user_id,
        session_id="s_1",
        ts=ts or dt.datetime.now(),
        payload=payload,
    )
    return event.to_json()


@requires_redis
def test_process_event_dispatches_and_writes_to_redis(redis_client, user_id):
    handlers = discover_handlers()
    state = SessionState(redis_client)
    metrics = Metrics()

    raw = _event_json(user_id, "destination_entered", {"category": "travel", "geo": "sea"})
    event = process_event(raw, handlers, redis_client, state, metrics)

    assert event is not None
    assert event.event_type == "destination_entered"

    value = redis_client.get(f"feature:user:{user_id}:user_current_destination_category")
    assert json.loads(value) == "travel"
    active = redis_client.get(f"feature:user:{user_id}:user_session_active")
    assert json.loads(active) is True

    ttl = redis_client.ttl(f"feature:user:{user_id}:user_current_destination_category")
    assert 0 < ttl <= 1800

    redis_client.delete(f"feature:user:{user_id}:user_current_destination_category")
    redis_client.delete(f"feature:user:{user_id}:user_session_active")


@requires_redis
def test_process_event_accepts_bytes_value(redis_client, user_id):
    handlers = discover_handlers()
    state = SessionState(redis_client)
    metrics = Metrics()

    raw = _event_json(user_id, "session_start", {}).encode("utf-8")
    event = process_event(raw, handlers, redis_client, state, metrics)
    assert event is not None
    redis_client.delete(f"feature:user:{user_id}:user_session_active")


@requires_redis
def test_process_event_unknown_type_does_not_crash_and_is_counted(redis_client, user_id):
    handlers = discover_handlers()
    state = SessionState(redis_client)
    metrics = Metrics()

    # promo_viewed is a real, handled type as of Phase 2 AC7 - use a type
    # that is deliberately never given a handler (same reasoning as
    # datagen/replay.py's __replayer_unknown_event_sentinel__)
    raw = _event_json(user_id, "__test_unknown_event_type__", {"promo_id": "p1"})
    event = process_event(raw, handlers, redis_client, state, metrics)

    assert event is None  # unknown types return None, not an exception
    snapshot = metrics.snapshot()
    assert snapshot["unknown_events_by_type"] == {"__test_unknown_event_type__": 1}
    assert snapshot["events_consumed_total"] == 0

    # nothing was written to redis for an unknown type
    assert redis_client.get(f"feature:user:{user_id}:__test_unknown_event_type__") is None


@requires_redis
def test_process_event_records_metrics(redis_client, user_id):
    handlers = discover_handlers()
    state = SessionState(redis_client)
    metrics = Metrics()

    process_event(_event_json(user_id, "session_start", {}), handlers, redis_client, state, metrics)
    process_event(
        _event_json(user_id, "destination_entered", {"category": "food", "geo": "x"}),
        handlers,
        redis_client,
        state,
        metrics,
    )

    snapshot = metrics.snapshot()
    assert snapshot["events_consumed_total"] == 2
    assert snapshot["events_consumed_by_type"] == {"session_start": 1, "destination_entered": 1}
    assert snapshot["processing_latency_ms_samples"] == 2
    assert snapshot["lag_seconds"] >= 0

    redis_client.delete(f"feature:user:{user_id}:user_session_active")
    redis_client.delete(f"feature:user:{user_id}:user_current_destination_category")
