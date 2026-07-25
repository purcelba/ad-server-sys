"""Phase 2 acceptance criteria (phases.md). All 7, run through per this
session's clarified instruction (no pause-before-AC checkpoint).

Requires `make up` (redpanda, redis) — self-skips without it.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import threading
import time
import uuid

import pytest
import redis as redis_lib
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from adserver.common.events import TOPIC, SessionEvent
from adserver.datagen.replay import run_replay
from adserver.stream_features import consumer as consumer_module
from adserver.stream_features.consumer import create_app, run_consume_loop
from adserver.stream_features.metrics import Metrics

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=5)
        redis_lib.Redis(host="localhost", port=6379).ping()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(
    not _infra_reachable(), reason="redpanda/redis not reachable — run `make up`"
)


@pytest.fixture
def redis_client():
    return redis_lib.Redis(host="localhost", port=6379, decode_responses=True)


class ConsumerHandle:
    """Starts a real consume loop on a background thread for the duration
    of a test, with its own Metrics instance."""

    def __init__(self):
        self.metrics = Metrics()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=run_consume_loop, args=(self.metrics, self._stop_event), daemon=True
        )

    def start(self, warmup_sec: float = 2.0):
        self._thread.start()
        time.sleep(warmup_sec)
        return self

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=10)


def _cleanup_user_keys(redis_client, user_id: str):
    for key in redis_client.scan_iter(f"feature:user:{user_id}:*"):
        redis_client.delete(key)
    for key in redis_client.scan_iter(f"window:{user_id}:*"):
        redis_client.delete(key)


# --- AC1 ---
# "Replaying events at 50 events/sec -> destination_category feature
# visible in Redis within 2 seconds of the event."


@requires_infra
def test_ac1_destination_category_visible_within_2s_under_50eps_load():
    handle = ConsumerHandle().start()
    tracer_user = f"ac1tracer_{uuid.uuid4().hex[:8]}"

    replay_thread = threading.Thread(
        target=run_replay, kwargs={"events_per_sec": 50, "duration_sec": 4, "seed": 1}, daemon=True
    )
    replay_thread.start()
    time.sleep(1.5)  # let load ramp up before firing the tracer

    producer = Producer({"bootstrap.servers": "localhost:9092"})
    tracer = SessionEvent(
        event_id=str(uuid.uuid4()),
        event_type="destination_entered",
        user_id=tracer_user,
        session_id="ac1-session",
        ts=dt.datetime.now(),
        payload={"category": "travel", "geo": "sea"},
    )
    publish_time = time.time()
    producer.produce(TOPIC, value=tracer.to_json().encode("utf-8"))
    producer.flush(5)

    deadline = publish_time + 2.0
    key = f"feature:user:{tracer_user}:user_current_destination_category"
    poll_client = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    found_at = None
    while time.time() < deadline:
        if poll_client.get(key) is not None:
            found_at = time.time()
            break
        time.sleep(0.05)
    elapsed = (found_at - publish_time) if found_at else (time.time() - publish_time)

    replay_thread.join(timeout=10)
    redis_client = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    result = redis_client.get(key)
    _cleanup_user_keys(redis_client, tracer_user)
    handle.stop()

    assert result is not None, f"destination_category never appeared in Redis within {elapsed:.2f}s"
    assert json.loads(result) == "travel"
    assert elapsed <= 2.0, f"took {elapsed:.2f}s, exceeds the 2s requirement"


# --- AC2 ---
# "TTL test: 30 min after session events stop (clock-mockable), session
# features are absent, not stale."
#
# Rather than literally sleeping 30 real minutes, SESSION_TTL_SECONDS is
# monkeypatched to a short value - "clock-mockable" is satisfied by making
# the TTL itself injectable, which exercises the exact same Redis EXPIRE
# mechanism process_event() uses in production, just on a fast clock.


@requires_infra
def test_ac2_session_features_absent_not_stale_after_ttl(monkeypatch, redis_client):
    from adserver.stream_features.state import SessionState
    from adserver.stream_features.consumer import discover_handlers, process_event

    monkeypatch.setattr(consumer_module, "SESSION_TTL_SECONDS", 2)

    handlers = discover_handlers()
    state = SessionState(redis_client)
    metrics = Metrics()
    user_id = f"ac2test_{uuid.uuid4().hex[:8]}"

    event = SessionEvent(
        event_id=str(uuid.uuid4()), event_type="session_start", user_id=user_id,
        session_id="s1", ts=dt.datetime.now(), payload={},
    )
    process_event(event.to_json(), handlers, redis_client, state, metrics)

    key = f"feature:user:{user_id}:user_session_active"
    assert redis_client.get(key) is not None  # present immediately after the event

    time.sleep(3)  # past the 2s TTL

    assert redis_client.get(key) is None  # absent, not a stale True value still sitting there


# --- AC4 ---
# "Lag and throughput visible via /metrics while replaying."


@requires_infra
def test_ac4_lag_and_throughput_visible_via_metrics_while_replaying():
    from fastapi.testclient import TestClient

    handle = ConsumerHandle().start()
    app = create_app(handle.metrics)
    client = TestClient(app)

    before = client.get("/metrics").json()
    assert before["events_consumed_total"] == 0

    run_replay(events_per_sec=50, duration_sec=2, seed=2)
    time.sleep(1)

    after = client.get("/metrics").json()
    handle.stop()

    assert after["events_consumed_total"] > before["events_consumed_total"]
    assert "lag_seconds" in after
    assert after["processing_latency_ms_avg"] >= 0
    assert set(after["events_consumed_by_type"]).issubset(
        {"session_start", "destination_entered", "ride_type_selected", "app_screen_view"}
    )


# --- AC6 ---
# "Unknown-event test: replay a stream containing a novel event type ->
# consumer neither crashes nor stalls, known features keep updating, and
# the unknown type appears in the metrics counter."


@requires_infra
def test_ac6_unknown_event_type_does_not_crash_or_stall_consumer():
    handle = ConsumerHandle().start()

    # unknown_event_rate=0.3 guarantees a meaningful mix of the permanent
    # sentinel type (unrecognized, by design - see replay.py) alongside
    # the known types in the same stream
    SENTINEL = "__replayer_unknown_event_sentinel__"
    count = run_replay(events_per_sec=40, duration_sec=3, seed=3, unknown_event_rate=0.3)
    time.sleep(1)

    snapshot = handle.metrics.snapshot()
    handle.stop()  # must still be alive to stop cleanly - proves no crash/stall

    assert count > 0
    assert SENTINEL in snapshot["unknown_events_by_type"]
    assert snapshot["unknown_events_by_type"][SENTINEL] > 0
    # known types kept updating despite the unknown ones interleaved
    known = set(snapshot["events_consumed_by_type"])
    assert known & {"session_start", "destination_entered", "ride_type_selected", "app_screen_view"}
    assert snapshot["events_consumed_total"] > 0


# --- AC5 ---
# "Manually fired event from the mini page is visible in Redis (correct
# feature, correct TTL) within 2 seconds - verified by hand and by one
# automated test hitting the publish endpoint."


@requires_infra
def test_ac5_mini_page_event_visible_in_redis_within_2s():
    from fastapi.testclient import TestClient

    from adserver.ui.publish_api import create_app as create_publish_app

    handle = ConsumerHandle().start()
    client = TestClient(create_publish_app())
    user_id = "u_0002"  # a real user from users.parquet, per the mini page's dropdown

    publish_time = time.time()
    resp = client.post(
        "/events",
        json={"user_id": user_id, "event_type": "ride_type_selected", "payload": {"ride_type": "shared"}},
    )
    assert resp.status_code == 200

    redis_client = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"feature:user:{user_id}:user_current_ride_type"
    deadline = publish_time + 2.0
    found = None
    while time.time() < deadline:
        found = redis_client.get(key)
        if found is not None:
            break
        time.sleep(0.05)
    elapsed = time.time() - publish_time

    ttl = redis_client.ttl(key)
    _cleanup_user_keys(redis_client, user_id)
    handle.stop()

    assert found is not None, f"never appeared within {elapsed:.2f}s"
    assert json.loads(found) == "shared"
    assert elapsed <= 2.0
    assert 0 < ttl <= 1800  # correct TTL - not expired, not unbounded


# --- AC7: streaming extensibility proof ---
# "add a handler for a new event type (e.g. promo_viewed ->
# promos_viewed_10min) touching only its own handler module + registry
# entries (+ event schema); the feature flows through to the Phase 3
# feature service with no other code changes."
#
# promo_viewed handler (stream_features/handlers/promo_viewed.py) added:
# one handler module + one registry.yaml entry + one EVENT_TYPES entry
# (event schema addition, explicitly allowed by "(+ event schema)").
# Zero edits to consumer.py/framework.py/state.py.
#
# The "served by Phase 3" clause can't be verified yet - feature_service/
# doesn't exist. Same caveat as Phase 1's AC5.


@requires_infra
def test_ac7_new_handler_auto_discovered_with_no_consumer_core_changes():
    from adserver.stream_features.consumer import discover_handlers

    handlers = discover_handlers()
    assert "promo_viewed" in handlers
    assert handlers["promo_viewed"].outputs() == ["promos_viewed_10min", "user_session_active"]


@requires_infra
def test_ac7_promo_viewed_flows_through_the_full_consumer(redis_client):
    from adserver.common.registry import load_registry
    from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH

    registry = load_registry(DEFAULT_REGISTRY_PATH)
    assert "promos_viewed_10min" in registry
    assert registry["promos_viewed_10min"].entity == "user"

    handle = ConsumerHandle().start()
    user_id = f"ac7test_{uuid.uuid4().hex[:8]}"

    producer = Producer({"bootstrap.servers": "localhost:9092"})
    event = SessionEvent(
        event_id=str(uuid.uuid4()), event_type="promo_viewed", user_id=user_id,
        session_id="s1", ts=dt.datetime.now(), payload={"promo_id": "promo_1"},
    )
    producer.produce(TOPIC, value=event.to_json().encode("utf-8"))
    producer.flush(5)

    key = f"feature:user:{user_id}:promos_viewed_10min"
    deadline = time.time() + 2.0
    found = None
    while time.time() < deadline:
        found = redis_client.get(key)
        if found is not None:
            break
        time.sleep(0.05)

    snapshot = handle.metrics.snapshot()
    _cleanup_user_keys(redis_client, user_id)
    handle.stop()

    assert found is not None
    assert json.loads(found) == 1
    # this specific event type must NOT be in unknown_events_by_type -
    # it's a real, handled type now
    assert "promo_viewed" not in snapshot["unknown_events_by_type"]
    assert snapshot["events_consumed_by_type"].get("promo_viewed", 0) > 0


# --- AC3: kill test (required) ---
# "stop the consumer 2 minutes mid-replay, restart -> consumer catches
# up; lag metric visibly spikes and recovers; a test asserts recovery
# and no crash."
#
# Scaled down from a literal 2 real minutes to ~12s of downtime - what's
# being verified (offset-committed resume + lag spike-then-recovery) is a
# property of the consumer-group mechanism, not of the specific downtime
# duration; 12s is long enough to build a real backlog and observe a
# genuine spike/recovery without a 2-minute test. "Stop"/"restart" is a
# real thread stop+join (stop_event.set() + join()), not just clearing
# state - the consumer group's offset commit, which is what actually
# needs to survive a restart, lives in Kafka, not the Python process, so
# this exercises the same recovery path an OS-level process kill would.


@requires_infra
def test_ac3_kill_test_lag_spikes_and_recovers_no_crash_no_data_loss():
    canary_user = f"ac3canary_{uuid.uuid4().hex[:8]}"
    downtime_sec = 12

    # --- phase 1: consumer running, replay flowing, confirm caught up ---
    handle = ConsumerHandle().start()
    replay_thread = threading.Thread(
        target=run_replay,
        kwargs={"events_per_sec": 15, "duration_sec": downtime_sec + 15, "seed": 4},
        daemon=True,
    )
    replay_thread.start()
    time.sleep(3)
    lag_before = handle.metrics.snapshot()["lag_seconds"]

    # --- phase 2: "kill" the consumer - stop_event + join, a real thread
    # stop, not just resetting in-memory state. Kafka's committed offset
    # (not this process) is what needs to survive for correct recovery.
    handle.stop()
    assert not handle._thread.is_alive()  # cleanly stopped, no hang

    # publish one canary event partway through the downtime, while nothing
    # is consuming - proves at-least-once: no message loss across the outage
    time.sleep(downtime_sec / 2)
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    canary = SessionEvent(
        event_id=str(uuid.uuid4()), event_type="destination_entered", user_id=canary_user,
        session_id="ac3-canary", ts=dt.datetime.now(), payload={"category": "retail", "geo": "sea"},
    )
    producer.produce(TOPIC, value=canary.to_json().encode("utf-8"))
    producer.flush(5)
    time.sleep(downtime_sec / 2)  # rest of the downtime - backlog keeps building

    # --- phase 3: restart, observe the spike, then wait for recovery ---
    # Processing is fast enough (~1ms/event) that the whole backlog can
    # drain in well under a second - a single sample taken after a fixed
    # warmup can miss the spike entirely by arriving after it already
    # recovered. Instead: start the thread directly (no warmup sleep) and
    # poll tightly from the first instant, tracking the peak lag seen.
    restarted = ConsumerHandle()
    restarted._thread.start()
    peak_lag_seen = 0.0
    poll_until = time.time() + 5.0
    while time.time() < poll_until:
        peak_lag_seen = max(peak_lag_seen, restarted.metrics.snapshot()["lag_seconds"])
        time.sleep(0.01)
    lag_right_after_restart = peak_lag_seen

    caught_up = False
    recovery_deadline = time.time() + 20
    lag_after_recovery = None
    while time.time() < recovery_deadline:
        lag_after_recovery = restarted.metrics.snapshot()["lag_seconds"]
        if lag_after_recovery < 2.0:
            caught_up = True
            break
        time.sleep(0.5)

    replay_thread.join(timeout=20)

    redis_client = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    canary_key = f"feature:user:{canary_user}:user_current_destination_category"
    canary_value = redis_client.get(canary_key)

    final_snapshot = restarted.metrics.snapshot()
    restarted.stop()
    assert not restarted._thread.is_alive()  # no crash, no hang on shutdown either

    _cleanup_user_keys(redis_client, canary_user)

    # lag was low before the outage
    assert lag_before < 2.0, f"pre-outage lag was already high: {lag_before}"
    # lag visibly spiked - the backlog built up while "dead"
    assert lag_right_after_restart > lag_before, (
        f"expected a visible spike, got lag_right_after_restart={lag_right_after_restart} "
        f"vs lag_before={lag_before}"
    )
    # and recovered
    assert caught_up, f"lag never recovered below 2.0s within 20s (last={lag_after_recovery})"
    # no data loss: the canary published during the outage was still processed
    assert canary_value is not None, "canary event published during downtime was lost"
    assert json.loads(canary_value) == "retail"
    # no crash: the restarted consumer kept processing after recovering
    assert final_snapshot["events_consumed_total"] > 0
