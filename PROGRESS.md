# Progress log

Per `CLAUDE.md`'s per-phase loop: one entry per completed, tagged phase —
what was built, decisions made, deviations from spec. Re-entry point after
gaps between sessions.

## Phase 0 — Skeleton and synthetic world (`phase-0`)

**Built:** `adserver/` repo scaffold (component READMEs for every
not-yet-built piece, stating what it will do and which phase owns it);
`docker-compose.yml` (Redpanda, DynamoDB-local, Redis, pinned versions,
healthchecks) + `make up/down/test/demo/eda`; `datagen/` generating 50
users (7 segments), 40 campaigns (5 categories x 2 demand types, 4 each),
and ~23k impression/click events over a 30-day window with planted
segment x category (x time-of-day) click lift; a lift-factor table
(`datagen/lifts.py`) rendered into both `datagen/README.md` and the repo
README with a drift test keeping them in sync; three EDA plots (CTR by
segment x category, CTR by segment x day/night bucket, and a raw
click-volume-by-hour diagnostic) confirming the planted signal is visible
in generated output, not just asserted; a `test_infra_healthy`
cross-component check. 24 tests, all passing against live infra.

**Decisions made (not previously locked in `CLAUDE.md`/`phases.md`):**
- `events.parquet` schema (no locked schema existed): `event_id`,
  `event_type`, `user_id`, `campaign_id`, `category` (denormalized),
  `segment` (denormalized), `ts`, `event_date`, `hour_of_day`, `click_id`
  (nullable FK from click rows to their impression). Treat as locked going
  forward, same as `users.parquet`/`campaigns.parquet`.
- 7 user segments (commuter, traveler, nightlife, foodie, shopper,
  homebody, general) — the spec named only "commuter, traveler,
  nightlife, etc."; `homebody` and `general` were added as explicit
  control groups for later model sanity-checks (Phase 4 AC2).
- Added a 4th Phase 0 acceptance criterion (EDA visualizations) at the
  user's request mid-phase; `phases.md` was updated to match before
  implementation.

**Deviations, diagnosed and fixed:**
- The initial `homebody` suppression lift (0.7x) was real (verified
  directly against `click_probability()`) but too small an effect to
  reliably distinguish from sampling noise at this data volume — a
  statistical smoke test failed on that basis, not a generator bug. Fixed
  by strengthening the lift to 0.3x (a genuinely distinguishable
  low-signal control) rather than padding data volume further or picking
  a more favorable seed.
- The user flagged the hour-of-day EDA plot as not looking right. Diagnosis:
  raw click counts per (segment, hour) cell were single digits even at
  ~13k total events — 24-way hourly granularity is inherently too sparse
  for CTR to read as signal rather than noise, since the lift table only
  conditions on a coarse day/night split in the first place. Fixed by (a)
  replacing the hourly CTR plot with a day/night-bucketed one, matching
  the granularity the signal is actually planted at, (b) adding a raw
  click-volume-by-hour plot as an explicit diagnostic showing why hourly
  is sparse, and (c) increasing event volume (10 → 18 mean
  impressions/user/day, ~13k → ~23k events) for headroom. A new test
  (`test_nightlife_night_ctr_beats_nightlife_day`) locks in that the
  bucketed view actually surfaces the signal.

**Not addressed (flagged for Phase 1):** `user_rides_per_week` (a Phase 1
batch feature named in `phases.md`) has no natural source in an ads event
log — Phase 1 will need to either add a synthetic rides signal or redefine
the feature.

**Amendment (during Phase 1 planning, flagged before making the change):**
resolved the `user_rides_per_week` gap by adding `rides.parquet` to
`datagen/` — a new table (`ride_id`, `user_id`, `ts`, `ride_date`,
`ride_type`), Poisson-generated with a segment-dependent daily rate
(commuter 2.5, traveler 1.5, nightlife/foodie/shopper/general 1.0, homebody
0.3 — mirroring the ads lift design so homebody stays a low-engagement
control across every signal). Decided after discussing the production
pattern directly: real-time "current ride" state and long-term "ride
history" aggregates are different concerns with different store
requirements (see `CLAUDE.md`'s "Real-time state vs. long-term aggregates"
section) — `user_rides_per_week` is a rolling aggregate, so it's squarely a
*batch* concept, independent of any future real-time ride-state feature.
This required touching Phase 0's already-tagged code; per `CLAUDE.md`'s
standing instructions, the change was flagged and discussed with the user
before being made, and `phase-0`'s tag was not moved for it — it's
recorded here as an explicit amendment instead. `phases.md`'s Phase 0 build
item and AC2 were updated to mention `rides.parquet`; all Phase 0 tests
(now 26) still pass.

## Phase 1 — Feature registry + batch feature pipeline (`phase-1`)

**Built:** `common/registry.yaml` + `registry.py` (10 features declared —
9 original + `user_account_age_days`, added as the AC5 extensibility
proof; loader validates required fields, entity/dtype enums, freshness
SLA format, raising `RegistryError` naming the offender); `common/audiences.yaml`
+ `audiences.py` (2 named, versioned audiences as ANDed rules over
registry features, same governance pattern as the registry);
`batch_features/framework.py`'s `FeatureJob` interface + `runner.py`'s
auto-discovering runner (`pkgutil`-based, no hand-maintained job list);
10 jobs in `jobs/` covering every registry feature, with shared
point-in-time computation helpers in `jobs/_shared.py`; `quality.py`'s
data-quality gate (row-count + null-rate, job-specific expected-entity-count
denominators); `offline_store.py`'s DuckDB point-in-time query layer over
the date-partitioned Parquet store; `materialize.py`'s single-table
DynamoDB-local materialization, independently registry-validated at the
write boundary; `reach.py` + `make reach`; `cli.py` + `make features`.
All 5 build items and all 5 acceptance criteria checked off in
`phases.md`. 83 tests, all passing against live infra.

**Decisions made (not previously locked in `CLAUDE.md`/`phases.md`):**
- `events.parquet`'s CTR/impression jobs report `null` (not a fabricated
  zero) for entities with no data in a window — nulls get resolved to the
  registry default at read time in Phase 3, per the defaults policy.
  `campaign_spend_yesterday` is the deliberate exception: it explicitly
  backfills every campaign to `0.0`, since a guaranteed campaign with no
  impressions yesterday has a real, known-zero spend, not an unknown one.
- Data-quality row-count denominators are job-specific, not "every entity
  in the catalog": ad-level windowed jobs (`ad_ctr_7d`/`30d`,
  `ad_impressions_7d`) count only campaigns whose flight is `active` and
  overlaps the window — verified directly (22/40 campaigns have 7d
  impressions, but that's 22/22 of campaigns actually eligible).
- Offline store partitions are queried via DuckDB SQL with hive-partition
  discovery (`available_partitions()`/`query_as_of()` in `offline_store.py`)
  — added after noticing the original build only wrote point-in-time-correct
  partitions but never queried them back, despite the build item explicitly
  naming DuckDB as the query mechanism.

**Deviations, diagnosed and fixed:**
- **Row-count gate blind to null entity IDs.** While building AC3's
  poisoning test, found that Polars' `group_by()` turns a corrupted
  (nulled) id column into its own spurious group, which the row-count
  check was counting as real coverage — a corrupted campaign could vanish
  entirely while the gate reported 100%. Fixed in `runner.py`: rows with
  a null entity id are dropped before either the quality gate or
  `materialize()` see them. Also would have let a literal `"ad#None"` key
  reach DynamoDB.
- **`materialize()` didn't independently enforce the registry.** While
  building AC4, found that "nothing outside the registry gets
  materialized" was only true because `runner.run()` happened to validate
  first — `materialize()`, the actual DynamoDB write boundary, trusted
  whatever `feature_names` it was given. Fixed: it now loads the registry
  itself and validates every feature name + entity before writing
  anything, raising `MaterializeError` otherwise.
- **AC3's poisoning test doesn't trip on the real dataset.** Corrupting
  one real day's events at 50% nulls does not fail either quality check
  at this data volume — a single day is too small a fraction of any job's
  window, and `campaign_spend_yesterday` is structurally immune regardless
  of severity (verified directly by running the corrupted case). Rather
  than retune production thresholds as a side effect of writing a test,
  AC3 was proven against a small, deliberately sized synthetic dataset
  instead — same approach as AC2's point-in-time proof.
- **Test-pollution in the shared `dynamodb-local` table.** Mutation-testing
  the AC3 and AC4 fixes (deliberately reverting each, confirming the test
  then fails, restoring, confirming it passes again) surfaced that fixed
  test IDs (`c_1`/`c_2`, `u_ac4_test`) collided with leftover items from
  the deliberately-broken runs. Fixed by switching to per-invocation
  unique IDs (`uuid4`) in both tests.

**Not addressed / deferred:**
- AC5's "feature is served by Phase 3" clause is unverifiable —
  `feature_service/` doesn't exist yet. Noted explicitly in `phases.md`
  rather than marked done; re-verify once Phase 3 is built.
- Streaming feature compute (Phase 2) is expected to honor the same
  registry contract per the build item's extensibility framing, but
  nothing here builds toward that yet — out of scope until Phase 2.

## Phase 2 — Real-time feature path (`phase-2`)

**Built:** `common/events.py`'s `SessionEvent` (envelope + typed payload),
shared verbatim by both producers and the consumer; `common/registry.yaml`
gained 5 real-time features (`user_session_active`,
`user_current_destination_category`, `user_current_ride_type`,
`user_screens_viewed_10min`, and AC7's `promos_viewed_10min`), all
`entity: user`, `freshness_sla` in seconds; `common/registry.py`'s
`VALID_DTYPES` gained `bool`. `stream_features/framework.py`'s
`EventHandler` interface (symmetric to Phase 1's `FeatureJob`) +
`state.py`'s Redis-backed `SessionState` (sliding-window counts via a
Redis sorted set, kept in Redis rather than consumer in-memory so it
survives a restart) + 5 handlers (4 original event types + AC7's
`promo_viewed`). `consumer.py`: `discover_handlers()` (auto-discovery,
mirroring `batch_features/runner.py`), `process_event()` (dispatch,
unknown-event handling, Redis writes — directly testable without a
running consumer), `run_consume_loop()` (real `confluent-kafka` consumer
on a background thread, at-least-once via default auto-commit), `/health`
+ `/metrics` via FastAPI/uvicorn. `datagen/replay.py` (the load/test
harness — realistic session modeling, configurable rate, optional
timestamp compression, `--unknown-event-rate` for injecting a novel
type) and `ui/mini.html` + `ui/publish_api.py` (the precision instrument)
— both publish the identical schema to the identical topic via
`ensure_topic()`'s idempotent creation, provably indistinguishable to the
consumer. All 5 build items and all 7 acceptance criteria checked off in
`phases.md`. 120 tests, all passing against live infra.

**New dependencies:** `confluent-kafka` (chosen over `kafka-python`: ~2x
the PyPI download volume, prebuilt macOS arm64/py3.12 wheel confirmed, no
source build needed), `redis`, `fastapi`, `uvicorn`, `httpx` (dev, for
FastAPI's `TestClient`).

**Decisions made (not previously locked in `CLAUDE.md`/`phases.md`):**
- `ride_type_selected` gets a real handler (`user_current_ride_type`)
  even though the build item's feature list didn't name one — the event
  type is schema-documented, so it belongs in the handler registry rather
  than falling into "unknown."
- Every handler refreshes `user_session_active`, not just `session_start`
  — any event means the session is still active; a session that only
  ever fires `destination_entered`/`ride_type_selected`/`app_screen_view`
  events (no explicit `session_start`, e.g. a client reconnect) still
  reads as active.
- Lag is defined as `wall_clock_now - event.ts` of the most recently
  processed message, per the build item's own wording ("latest event
  timestamp minus last processed event timestamp") — grows during an
  outage, shrinks once caught up, and needs no extra broker admin queries.
- AC2's TTL test uses dependency injection (`SESSION_TTL_SECONDS`
  monkeypatched short) rather than literally sleeping 30 real minutes —
  "clock-mockable" read as "make the TTL itself injectable," which
  exercises the identical Redis `EXPIRE` mechanism production uses.
- AC3's kill test downtime was scaled from a literal 2 minutes to ~12
  seconds — what's being verified (consumer-group offset-commit survives
  a restart; lag spikes then recovers) is a property of the mechanism,
  not of the specific downtime duration, and a real OS-level "stop the
  consumer" is well-approximated by a real thread stop+join, since the
  offset that actually needs to survive restart lives in Kafka, not the
  Python process.

**Deviations, diagnosed and fixed:**
- **AC3's first attempt missed the spike entirely.** Sampling lag once
  after a fixed 0.5s warmup post-restart showed no spike (`0.008` both
  before and "after") — not because recovery didn't happen, but because
  processing is fast enough (~1ms/event) that the whole backlog had
  already drained within that 0.5s window, before the sample was even
  taken. Confirmed via `rpk group describe` that the consumer group
  genuinely reached zero lag. Fixed by polling tightly (10ms) from the
  instant the restarted thread starts and tracking peak lag observed,
  rather than one delayed point-in-time read.
- **AC7 broke AC6 and a `test_consumer.py` test.** Both had used
  `promo_viewed` as their stand-in "unknown event type" — which stopped
  being true the moment AC7 gave it a real handler. Fixed by switching
  both to permanent sentinel type strings (`__replayer_unknown_event_sentinel__`,
  `__test_unknown_event_type__`) deliberately never given handlers, so
  future extensibility proofs can't silently break this pattern again.
  Caught by re-running the full suite after AC7 landed, not just the
  individual AC7 tests — the same discipline that caught Phase 1's
  hardcoded-registry-count breaks.
- **Two Phase 1 test files needed touching** (flagged per `CLAUDE.md`,
  since `phase-1` is tagged): AC1's test asserted materialized names ==
  *all* registry names, which broke once the registry held stream-only
  features no batch job produces — fixed to compare against what the
  discovered batch jobs actually claim. `test_invalid_dtype_raises` used
  `dtype: bool` as its example of an invalid dtype, which stopped being
  true once `bool` became a supported dtype — swapped to `dtype: datetime`.

**Not addressed / deferred:**
- AC7's "feature is served by Phase 3" clause is unverifiable —
  `feature_service/` doesn't exist yet. Same caveat as Phase 1's AC5.
- Batch and streaming compute now both honor the same registry, but
  nothing yet unifies *reading* them into one governed API — that's
  Phase 3's `feature_service/`.
