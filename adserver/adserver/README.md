# adserver (serving endpoint)

## SLO (written before code, per `phases.md`'s Phase 5 build item)

- **Latency:** p99 ≤ 100ms for `POST /serve`, measured end to end
  (retrieval → feature fetch → scoring → bidder → arbitration → response).
  Per-stage budgets that add up under that ceiling: feature fetch ≤ 20ms
  (a hard `httpx` timeout against `feature_service`), external bidder ≤
  30ms (a hard `httpx` timeout against `bidder_stub`) — everything else
  (retrieval, scoring, arbitration, logging) is in-process and expected to
  be single-digit milliseconds given this project's data volume.
- **Availability:** 99.9% — meaning `POST /serve` returns a valid response
  (a winning ad or a house ad) for 999 out of 1000 requests regardless of
  any single dependency's health. This is a design constraint on the
  degradation ladder below, not just a monitoring target: every dependency
  call is wrapped so its own failure degrades to the next rung rather than
  propagating up. **A request never returns a 500** in any of the three
  defined degraded modes.
- **Defined degraded modes**, in priority order (worse dependency failures
  degrade further down the ladder; each rung is logged in the decision log
  when it fires):
  1. **Real-time features stale/missing** → serve on batch-only feature
     values. `feature_service`'s own per-feature `freshness_status`
     already carries this signal in its response — no extra health check
     needed; the affected candidate(s) score using whatever value came
     back (batch fallback or registry default), and which features
     degraded is recorded per candidate in the decision log.
  2. **`feature_service` unreachable or times out** (the whole call, not
     one feature) → fall back to a cached, periodically-refreshed
     in-process popularity ranking (candidates ordered by their last-known
     `ad_ctr_30d`), skipping personalized scoring entirely for this
     request.
  3. **Model/scorer load failure** (the live `PctrModel` artifact is
     missing, corrupt, or fails to load) → serve a fixed, always-eligible
     house ad. This is the last rung — a house ad never fails to be
     eligible, so this is the terminal fallback before "no response at
     all," which the SLO forbids.

## What it does
*(filled in as each build item lands)*

## How to run and test it alone
*(filled in as each build item lands)*

## Production analog
This is the local stand-in for the actual ad-ranking/serving tier — the
service a real ads org would run behind a load balancer, typically with
per-stage timeouts and circuit breakers to exactly the dependencies this
project models (a feature store, a ranking model service, an external
demand partner). The degradation ladder mirrors a real production
practice: prefer a degraded-but-real response over an outage, with every
degradation observable (logged, not silent).

## Ownership note
Under an end-to-end ads team model, `adserver/adserver/` is squarely ads
team territory — it's the actual product surface (what ad gets shown),
not shared infrastructure another team could plausibly own instead.
