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

(The replayer and mini page — the two event sources — are still to come
within this phase; the "Replayer vs. mini page" section will land here
once both exist.)

## How to run and test it alone
```bash
uv run pytest adserver/stream_features -v   # needs `make up` for most tests; self-skips without it
uv run python -m adserver.stream_features.consumer   # runs the consumer standalone, serves :8001
```

## Production analog
This is the local, plain-Python stand-in for a production stream
processor — e.g. Kinesis/Kafka + Flink, with a real online feature store
(Redis/DynamoDB-backed) as the sink. This project deliberately uses a
plain Python consumer instead of Flink (`phases.md`'s locked decision);
the README section this note lives in (once the replayer/mini page land)
maps stream-processing concepts — event time vs. processing time,
windows, state, watermarks — to where each appears in this code.

## Ownership note
Under an end-to-end ads team model, the handlers (which product events
map to which features) are plausibly owned by the ads team — they know
what real-time signal ranking needs. The consumer core/framework is more
plausibly owned by a central ML/data platform team, for the same
consistency-across-teams reason as `batch_features/`'s runner.
