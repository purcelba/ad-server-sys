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

## Manual walkthrough & debugging

What actually happens when you fire one event from the mini page, and
what tool is doing what at each stage:

1. **Browser (`ui/mini.html`, plain JS)** — packages your form selections
   into JSON and `fetch()`s it to the publish API. Stands in for a real
   app's event-tracking code.
2. **FastAPI (`ui/publish_api.py`)** — receives the HTTP request, builds a
   proper event record (adds an ID, timestamp, session ID), does a quick
   sanity check (`validate_for_publish()`: is this a real event type?),
   and publishes it. Its whole job is "HTTP request → Kafka message."
3. **Redpanda** (a Kafka-compatible message broker) — the durable middle
   layer. A queue: publish_api drops the event in, and anyone subscribed
   can pick it up whenever they're ready. This decoupling is the point —
   the publisher doesn't need to know who's reading or how fast.
4. **`confluent-kafka` + the consumer process (`consumer.py`)** — a
   background process continuously pulling new messages off that queue,
   figuring out each one's `event_type`.
5. **The handler** (e.g. `handlers/ride_type_selected.py`) — small,
   type-specific logic deciding what feature value this event produces.
   Most are trivial; `app_screen_view`'s does a bit of sliding-window math.
6. **Redis** — where the finished feature value actually lands, keyed by
   user + feature name, with a 30-minute TTL. This is the "ready for
   online inference" endpoint — anything reading real-time signal at
   serving time would read from here, because it's fast enough to check
   on every request without adding noticeable delay.
7. **(Not built yet) `feature_service/`, Phase 3** — the governed API that
   would read Redis (+ DynamoDB for batch features) and hand a unified
   answer to whatever's making the ad-ranking decision. Right now you can
   *see* the feature sitting in Redis via a direct `GET`, but there's no
   serving layer in front of it yet.

`/metrics` isn't a stage in this journey — it's a window onto steps 4-6,
reporting throughput/latency/unknown-type counts. Useful for debugging,
not part of the data path itself.

**Watching it happen, live**, two terminals:
```bash
# terminal 1 - tail the raw topic (shows the event the instant it's published)
docker exec ad-server-sys-redpanda-1 rpk topic consume session_events

# terminal 2 - watch it land in Redis once processed (swap in your user/feature)
watch -n 1 "uv run python -c \"import redis; r=redis.Redis(host='localhost',port=6379,decode_responses=True); print(r.get('feature:user:u_0001:user_current_ride_type'))\""
```
Fire an event from the mini page; it should appear in terminal 1
immediately, and terminal 2 within about a second.

Some things worth knowing before you go looking for them:
- **The consumer log (`consumer.py`'s `logger` calls) stays silent on the
  happy path.** It only logs unusual things — unknown event types, errors
  — never a line per successfully-processed event. If you're expecting to
  see activity there per click, you won't; check `/metrics` or Redis
  instead (`curl -s http://localhost:8001/metrics | python3 -m json.tool`).
- **A one-time `WARNING: kafka error: ... Unknown topic or partition`** on
  consumer startup is expected, not a failure — the topic doesn't exist
  until the first message is ever published to it (Redpanda auto-creates
  on first `produce()`), and the consumer's subscribe can land a beat
  before that. It's handled non-fatally and self-resolves after the first
  publish; `datagen/replay.py::ensure_topic()` avoids it entirely for the
  replayer by creating the topic explicitly up front.
- **Kafka topics aren't queryable like a database** — no index on
  payload fields. "Find events for `u_0001`" means consuming and filtering
  client-side (e.g. `rpk topic consume -n 100 | jq 'select(...)'`), not a
  lookup.

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
