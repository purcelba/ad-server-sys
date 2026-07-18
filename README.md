# ad-server-sys

A learning project: a miniature real-time ad serving system (feature stores,
streaming, two-stage ranking, pacing, yield, degradation, decision logging,
experimentation), built phase by phase with Claude Code at local scale.

- See [`CLAUDE.md`](CLAUDE.md) for conventions, data schemas, and the
  per-phase working loop.
- See [`phases.md`](phases.md) for the full build spec, phase by phase, with
  acceptance criteria as checkboxes.

## Phase 0: planted synthetic signal

`datagen/` (see [`adserver/datagen/README.md`](adserver/datagen/README.md))
generates a synthetic catalog of users, ad campaigns, 30 days of
impression/click history where clicks follow a planted segment × category
(× time-of-day) preference, and 30 days of ride history with a planted
segment-dependent frequency (`rides.parquet`, added during Phase 1
planning as a flagged Phase 0 amendment — see `PROGRESS.md`). Click
probability = `base_ctr (3%) × lift(segment, category, hour_of_day)`,
sourced from `adserver/datagen/lifts.py::LIFT_TABLE`:

| segment | category | condition | lift |
|---|---|---|---|
| commuter | transit | any | 3.0x |
| commuter | retail | any | 1.3x |
| traveler | travel | any | 3.0x |
| traveler | retail | any | 1.2x |
| nightlife | entertainment | night (20:00–04:59) | 3.5x |
| nightlife | food | night (20:00–04:59) | 2.5x |
| nightlife | entertainment | day | 1.0x |
| foodie | food | any | 3.0x |
| shopper | retail | any | 3.0x |
| shopper | entertainment | any | 1.2x |
| homebody | all categories | any | 0.3x (suppressed — low-engagement control) |
| general | all categories | any | 1.0x (no lift — control group) |

Any (segment, category) pair not listed defaults to 1.0x. This table is
cross-checked against `lifts.py` by
`adserver/datagen/tests/test_lift_docs.py` so it can't silently drift.
