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

## Phase 7: the Streamlit debug app (`app.py`)

`uv run streamlit run adserver/ui/app.py` (`make ui`) — two tabs, both
reading live from whichever services are currently running rather than
from any stored state of their own.

**Tab 1, Rider** (`rider_tab.py`) — the mini page's successor, not its
replacement: `mini.html` still exists unchanged as the minimal precision
instrument. Rider adds: a "Serve ad" button (`POST :8005/serve`) and a
debug panel reading `decision_log.jsonl` for the request's candidate set,
per-candidate pCTR/eCPM, arbitration rung, external bid outcome,
fallback rung, and per-stage latency. Fires events through the *exact
same* `POST :8002/events` endpoint `mini.html` posts to (`publish_api.py`
never changed) — no second Kafka producer. Two "Serve ad" buttons (before
firing an event, and fire-then-serve) render side by side specifically so
a real-time signal's effect is a visible before/after diff, not a single
snapshot. A separate "current user feature freshness" panel calls
`feature_service` directly (`features.py`'s own `fetch_user_features()`)
rather than reading the decision log — `scoring.py`'s
`from_online_result()` unwraps `FeatureValue.value` before it ever gets
logged, so `freshness_status` doesn't survive into `decision_log.jsonl`
at all; a live call is the only way to show it.

**Tab 2, Ops** (`ops_tab.py`) — polls `adserver`'s and
`stream_features`' consumer's `/metrics` for request rate (`/metrics`
only reports cumulative counts, so the rate is computed by diffing two
polls — `logic.compute_rate()`), p99 latency by stage, fallback-rung
counts, and consumer lag; reuses `ops.reconcile.reconcile()` and
`ops.readout.per_arm_ctr()` directly (Python import, not a second HTTP
client) for spend/delivery-by-campaign and experiment-arm-split panels —
one implementation of each, same as Phase 6 already established between
its own consumers. A checkbox-gated `sleep()`+`st.rerun()` loop drives
auto-refresh — no new dependency for something a two-line loop already
does. Every panel polls and degrades independently: an unreachable
service shows "unreachable" for just that panel rather than crashing the
whole tab, since the Ops tab specifically needs to keep working through
the same failure scenarios (feature_service down, bidder down, Redis
down) it exists to make visible.

`logic.py` holds the handful of genuinely reusable, unit-testable
pieces both tabs lean on (decision-log lookup by `request_id`, the rate
computation, a campaign display-label helper) — kept separate from the
`st.*` rendering code specifically so it's testable without driving
Streamlit itself.

## How to run and test it alone
```bash
uv run pytest adserver/ui -v   # logic.py + publish_api tests; publish_api's self-skips without `make up`

# full stack, one terminal each (or background them):
make up
make serve-features   # :8003
make serve-bidder     # :8004
make serve-ads        # :8005
make consumer         # :8001, stream_features
make serve-events     # :8002, publish_api.py (mini.html + POST /events)
make ui                # :8501, the Streamlit app
```

## Production analog
`mini.html`/`publish_api.py` stand in for a real app client (or an
internal ops tool) publishing session events — in production this would
be the actual rider app's event SDK. The Streamlit app is the local
stand-in for whatever internal debug/observability tooling a real ads
team builds on top of its own request tracing and metrics — e.g. a
Grafana/Datadog dashboard for the Ops tab's panels, and an internal
"replay this request" tool for the Rider tab's debug trail. There's no
real analog for "a debug HTML form" or "a local Streamlit app" as
technologies — the underlying actions (publish one event, inspect one
request's full decision trail, watch live fallback/latency metrics) are
exactly what those real tools do.

## Ownership note
Under an end-to-end ads team model, all of `ui/` is plausibly owned by
the ads team itself — it's their debug instrument, covering their own
serving path and experiment. The one negotiable piece is the *general
shape* of the Ops tab's metrics-polling/dashboard pattern, if a central
tooling/platform team maintains a shared internal dashboarding framework
other teams are expected to plug into instead of hand-rolling their own
— the tradeoff is a bespoke, ads-specific tool (fast to build, exactly
fits this system) vs. a shared framework (consistent across teams, but a
dependency this project's local-only, no-Kubernetes footprint doesn't
otherwise have).
