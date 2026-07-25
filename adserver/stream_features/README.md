# stream_features

## What it does
Consumes session events from Redpanda and writes fresh, Redis-backed
online features per user, with a 30-minute TTL.

**Streaming handler interface** (`framework.py`): every handler
implements `event_type()`, `outputs()` (registry feature names), and
`update(event, state) -> {feature_name: value}`. Handlers own only
feature logic — the consumer core owns consumption, dispatch, Redis
writes, TTLs, and metrics. Symmetric to `batch_features/framework.py`'s
`FeatureJob`.

`state.py`'s `SessionState` is a thin Redis-backed helper for handlers
that need windowed state (currently just `app_screen_view`'s 10-minute
sliding count, via a Redis sorted set) — kept in Redis rather than
consumer in-memory, so it survives a consumer restart.

4 handlers in `handlers/`, one per event type: `session_start` →
`user_session_active`; `destination_entered` →
`user_current_destination_category`; `ride_type_selected` →
`user_current_ride_type`; `app_screen_view` →
`user_screens_viewed_10min`. Every handler also refreshes
`user_session_active` — any event means the session is still active.

**Consumer core** (`consumer.py`): `discover_handlers()` auto-discovers
every `EventHandler` subclass under `handlers/` (`pkgutil`-based, no
hand-maintained list — same extensibility seam as
`batch_features/runner.py`'s `discover_jobs()`). The consume loop
(`confluent-kafka`, consumer group `stream-features-consumer`, at-least-once
via default auto-commit) runs in a background thread; `process_event()`
— dispatch, unknown-event handling (counted, never a crash), Redis
writes — is directly callable without a running consumer, so it's
unit-testable in isolation. `/health` + `/metrics` (events consumed,
processing latency, lag, `unknown_events_by_type`) are served via
FastAPI/uvicorn in the main thread.

Verified end to end against real infra: a message published via
`confluent_kafka.Producer` is picked up by the real consume loop,
written to Redis, and reflected in `/metrics` — not just exercised
through `process_event()` directly.

## Replayer vs. mini page

Two event sources publish the identical `SessionEvent` schema
(`common/events.py`) to the identical `session_events` topic — the
consumer genuinely cannot tell them apart.

- **Replayer** (`datagen/replay.py`) is the **load and test harness**: it
  exists for volume, repeatability, and headless use. Every automated
  acceptance criterion needing sustained traffic (the kill test, load
  tests) runs on this. Models realistic sessions (`session_start` → 2-5
  middle events → naturally ends) via an active-session pool, drawing
  users from the real `users.parquet` catalog. `--unknown-event-rate`
  injects a novel event type for the unknown-event test;
  `--compression-factor` decouples simulated event time from wall-clock
  publish rate for generating volume/history quickly (default `1.0` =
  real-time — what the live-latency ACs need).
- **Mini page** (`ui/mini.html` + `ui/publish_api.py`) is the **precision
  instrument**: for firing a single, deliberate event by hand and
  watching it propagate through Redis. It's the primary tool for manual
  verification and demos from this phase onward — the Phase 7 Rider tab
  extends it, rather than replacing it.

Both go through `ensure_topic()` (idempotent topic creation) and the same
`SessionEvent.to_json()`/`validate_for_publish()` path before publishing,
so there's no schema drift possible between them.

## How to run and test it alone
```bash
uv run pytest adserver/stream_features -v   # needs `make up` for most tests; self-skips without it
uv run python -m adserver.stream_features.consumer   # runs the consumer standalone, serves :8001
uv run python -m adserver.datagen.replay --duration-sec 30   # load harness
uv run python -m adserver.ui.publish_api   # mini page at :8002
```

## Production analog
This is the local, plain-Python stand-in for a production stream
processor — e.g. Kinesis/Kafka + Flink, with a real online feature store
(Redis/DynamoDB-backed) as the sink. This project deliberately uses a
plain Python consumer instead of Flink (`phases.md`'s locked decision).
Concept mapping to real stream-processing terms:
- **Event time vs. processing time**: `SessionEvent.ts` is event time (set
  by the producer, when the thing actually happened); `process_event()`'s
  own `time.time()` calls are processing time (when this consumer got
  around to handling it). The `lag_seconds` metric is literally the gap
  between the two.
- **Windows**: `SessionState.record_and_count_window()` implements a
  10-minute sliding window directly (Redis sorted set + score-range
  pruning) — no windowing framework, just the primitive a real one would
  be built on.
- **State**: the sliding-window sorted sets *are* the consumer's state —
  kept in Redis specifically so it's not lost on a restart (see the kill
  test). A real Flink job would use RocksDB-backed state instead of an
  external store, but the concept (durable, per-key state a stream job
  reads and mutates) is the same.
- **Watermarks**: not implemented — this consumer has no notion of "no
  more events before time T are coming." At-least-once delivery is
  accepted as a locked decision instead (see below), which is a simpler,
  weaker guarantee than what watermark-based exactly-once systems provide.

## Key decisions locked
**At-least-once delivery accepted.** `confluent-kafka`'s default
auto-commit (~5s interval) means a crash between processing a message and
the next auto-commit can cause that message to be reprocessed after
restart. For these handlers that means, worst case, a feature gets
rewritten with the same or a very slightly stale value — not a
correctness problem, since every write is idempotent (last-write-wins on
a TTL'd key), except `user_screens_viewed_10min`'s sliding-window count,
where reprocessing the same `event_id` is also safe: `ZADD` on an
existing member just updates its score, it doesn't double-count. The
practical implication: **serving logs (Phase 5+), not the event stream
itself, are the measurement source of truth** for anything that must not
be double-counted (e.g. impression/spend accounting) — the stream is for
freshness, not for exactly-once bookkeeping.

**Plain Python consumer, no Flink.** A deliberate simplicity choice
(`phases.md`'s locked decision) — see the concept-mapping section above
for where Flink's core ideas (event/processing time, windows, state,
watermarks) show up here in a much smaller form.

## Ownership note
Under an end-to-end ads team model, the handlers (which product events
map to which features) are plausibly owned by the ads team — they know
what real-time signal ranking needs. The consumer core/framework is more
plausibly owned by a central ML/data platform team, for the same
consistency-across-teams reason as `batch_features/`'s runner.
