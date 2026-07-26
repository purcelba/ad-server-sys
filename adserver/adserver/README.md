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
`POST /serve {user_id, session_id, slot}` runs the full pipeline and
returns a winning ad (or a house ad, or nothing if genuinely nothing was
eligible):

1. **Candidate retrieval** (`retrieval.py`) — filters the campaign catalog
   to what's eligible: active status, flight covers today, remaining
   pacing capacity > 0, and the audience routing rule (a campaign that
   purchased targeting on an audience is ineligible for non-members;
   untargeted campaigns are unaffected either way — see the required code
   comment at the filter itself for why this must never become a
   relevance filter).
2. **Feature fetch** (`features.py`) — user features once per request
   (needed early for retrieval's audience check), candidate ad features
   batched for whatever survived retrieval, both over real HTTP to
   `feature_service` (`:8003`) with a 20ms budget — never an in-process
   import, per the project's "services communicate only via HTTP" rule.
3. **Scoring** (`scoring.py`) — assembles each candidate's feature dict
   via `ranking.assemble` (the identical function Phase 4's training path
   uses) and scores with the live `Scorer`; eCPM = bid × pCTR for auction
   candidates.
4. **External demand** (`bidder_stub/`) — a hard 30ms `httpx` timeout;
   timeout/failure means internal demand only, logged as such.
5. **Yield arbitration** (`pacing.py`) — a behind-schedule guaranteed
   campaign wins its slot outright; otherwise the slot goes to whichever
   of (best internal eCPM, the external bid) is higher.
6. **Degradation ladder** — wired directly into the request handler; see
   the SLO section above for the three rungs.

Every request is logged to `data/decision_log.jsonl` (`decision_log.py`)
and split into an A/B arm (`experiment.py`, `hash(salt:user_id)`, pins a
specific model version regardless of which one is `live`).

## Known deviation: AC1 load target
The phase-5 plan's original AC1 target was p99 ≤ 100ms at 50 RPS.
Measured with `ab` (a real concurrent C client — every Python-based load
client tried, including a thread-pool one paced at an exact target rate,
measured wildly worse and less consistent tail latency against this exact
server, the same class of GIL-contention artifact Phase 3 hit with an
async client) on this local dev machine: p50 in the 40-70ms range, p99 in
the 150-300ms range. That's after real, measured tuning:
`threadpoolctl.threadpool_limits(1)` (a single `scorer.score()` call for
v2/`HistGradientBoostingClassifier` measured ~2300ms average under 50
concurrent Python threads before this fix, ~23ms after — sklearn's
internal OpenMP/BLAS thread pool oversubscribing badly when many
concurrent request-handling threads each spin up their own internal
parallelism for a single-row prediction that doesn't need it), multiple
uvicorn worker processes, a raised AnyIO threadpool cap, and GC
threshold/freeze tuning (the fix that mattered most for `feature_service`
in Phase 3). The residual gap is attributed to this machine running
`feature_service` (8 workers) + this service (8 workers) + `bidder_stub`
+ Redis/DynamoDB-local containers simultaneously on a shared core count —
see `PROGRESS.md`'s phase-5 entry for the full trail.

## Known deviation: pacing overshoot (locked decision, not a bug)
`pacing.py`'s `decrement_capacity()` is a plain `GET` then `SET`, never an
atomic `DECRBY`/`INCRBY` or a Lua script. Two concurrent requests can both
read the same "1 remaining" value, both decide they're eligible, and both
serve — an overshoot of 2 delivered against a budget of 1.
`tests/test_acceptance.py::test_ac4_two_concurrent_requests_both_decrement_the_last_unit`
demonstrates this deterministically (a `threading.Barrier` forces the
exact race window rather than relying on real thread-scheduling luck).
This is a locked project decision (`phases.md`): the concurrency flaw is
a feature of the project, not a bug to fix — real production systems
avoid it one of a few ways:
- **Atomic decrement** (`DECRBY`/`INCRBY` in Redis, or a small Lua script
  for a check-and-decrement in one round trip) — closes the read-then-
  write race but still allows a decrement to go negative under enough
  concurrent pressure unless paired with a floor check inside the same
  atomic operation.
- **A reservation with rollback** — reserve a unit before serving,
  confirm after (release the reservation if the request is later
  discarded, e.g. the auction changes its mind) — the standard pattern
  for anything where "maybe served, maybe not" isn't acceptable.
- **Accept the overshoot and reconcile after the fact** — the approach
  this project actually takes: serve fast or best-effort, and let a
  downstream batch job (Phase 6's `ops/reconcile.py`) compare
  decision-log impressions against pacing counters and quantify the
  discrepancy. Real ad systems often do exactly this for pacing
  specifically, because a strictly-consistent counter on the hot serving
  path adds real latency for a problem that's rare in practice and cheap
  to detect/correct after the fact.

## How to run and test it alone
```bash
uv run pytest adserver/adserver -v          # needs `make up` for most tests; self-skips without it
make serve-bidder                            # bidder_stub, :8004
make serve-features                          # feature_service, :8003 (separate terminal)
make serve-ads                               # this service, :8005 (separate terminal)

curl -X POST localhost:8005/serve -H 'Content-Type: application/json' \
  -d '{"user_id": "u_0001", "session_id": "s1"}'

make loadtest-serve                          # paced-RPS Python client (see the AC1 deviation note above
                                              # for why its numbers aren't authoritative — ab is)
```

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
