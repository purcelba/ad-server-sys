# ui

## What it does
`mini.html` + `publish_api.py`: the **mini event page**, Phase 2's
precision instrument for firing one deliberate session event by hand and
watching it propagate. A single form (user dropdown, event type, and the
event-type-specific field — destination category / ride type / screen)
that `POST`s to `/events`, which builds a `SessionEvent` via the exact
same `common/events.py` constructors and publishes to the exact same
Kafka topic (`session_events`) the replayer uses — see
`stream_features/README.md`'s "Replayer vs. mini page" section for the
full division of labor between the two event sources.

Server-side validation (`validate_for_publish()`) rejects unknown event
types before they reach Kafka, same as any other producer.

(The Phase 7 Rider tab extends this page later — adds the served-ad debug
panel — rather than replacing it.)

## How to run and test it alone
```bash
uv run pytest adserver/ui -v   # needs `make up` (redpanda) — self-skips without it
uv run python -m adserver.ui.publish_api   # serves the form at :8002
```

## Production analog
Stands in for a real app client (or an internal ops tool) publishing
session events — in production this would be the actual rider app's
event SDK. There's no real analog for "a debug HTML form," but the
underlying action (publish one event to the stream) is exactly what a
real client does.

## Ownership note
Under an end-to-end ads team model, this tool is plausibly owned by the
ads team itself (it's their debug instrument) or negotiable with a
central tooling/platform team if similar debug pages exist for other
event-producing surfaces — the tradeoff is duplicated tooling per team
vs. a shared, less ads-specific debug UI.
