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
