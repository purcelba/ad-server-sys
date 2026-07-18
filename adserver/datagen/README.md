# datagen

## What it does
Generates the synthetic "world" the rest of the project runs against:
a catalog of users and ad campaigns, 30 days of impression/click history
where clicks follow a planted, documented signal (segment × category, with
a time-of-day condition for one segment), and 30 days of ride history with
a planted segment-dependent frequency. Output is four Parquet files:
`users.parquet`, `campaigns.parquet`, `events.parquet`, `rides.parquet`.

Generation is fully deterministic — a single `numpy.random.default_rng(seed)`
instance is threaded explicitly through user, campaign, event, and ride
generation in a fixed order; same seed → byte-identical output files.

`rides.parquet` was added during Phase 1 planning, after `phase-0` was
already tagged — a flagged amendment (see `PROGRESS.md`'s Phase 0 entry)
to give Phase 1's `user_rides_per_week` batch feature a real data source,
since it has no natural basis in ad impression/click events alone.

## How to run and test it alone
```bash
uv run python -m adserver.datagen.cli --seed 42 --out data/
uv run pytest adserver/datagen -v
```
`make demo` runs the generator with the default seed and prints a preview
of each file. `make eda` (depends on `make demo`) additionally writes CTR
visualizations to `data/eda/`.

## Production analog
At Lyft-scale this doesn't have a direct analog — it's a stand-in for the
real user/campaign catalogs and historical event warehouse (Kinesis/S3
event logs) that already exist in production. Its role here is purely to
give the rest of this project's phases known ground truth to validate
feature pipelines and model training against.

## Ownership note
N/A — this component only exists for this learning project; it has no
real-world ownership equivalent.

## Data schema
See `CLAUDE.md`'s "Data schemas" section for the locked `users.parquet`
and `campaigns.parquet` columns. `events.parquet` (defined in this phase):

| column | type | notes |
|---|---|---|
| `event_id` | str | unique |
| `event_type` | enum | `impression` \| `click` |
| `user_id` | str | FK → users |
| `campaign_id` | str | FK → campaigns |
| `category` | str | denormalized from campaign |
| `segment` | str | denormalized from user |
| `ts` | datetime | event timestamp within the 30-day window |
| `event_date` | date | derived from `ts` |
| `hour_of_day` | int 0–23 | denormalized from `ts` |
| `click_id` | str, nullable | for click rows, FK → the impression's `event_id` |

`rides.parquet` (defined during Phase 1 planning, a flagged Phase 0 amendment):

| column | type | notes |
|---|---|---|
| `ride_id` | str | unique |
| `user_id` | str | FK → users |
| `ts` | datetime | ride timestamp within the 30-day window |
| `ride_date` | date | derived from `ts` |
| `ride_type` | str (enum) | `standard` \| `shared` \| `premium`, uniform |

## Planted effects (segment × category lift factors)

Click probability = `base_ctr (3%) × lift(segment, category, hour_of_day)`,
capped at 60%. This table is rendered directly from
`adserver/datagen/lifts.py::LIFT_TABLE` — the single source of truth, so
it cannot drift from the actual generator behavior.

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

Any (segment, category) pair not listed above defaults to 1.0x (no lift).

`homebody` and `general` exist specifically as controls: `general` should
show no directional lift in any category, and `homebody` should show
uniformly suppressed engagement — useful later (Phase 4) for sanity-checking
that a trained model's learned lifts track this ground truth rather than
picking up spurious signal.

## Planted effects (ride frequency)

Rides/user/day are Poisson-distributed with a segment-dependent mean
(`adserver/datagen/rides.py::RIDES_PER_DAY_MEAN`), mirroring the ads lift
design so `homebody` stays a low-engagement control across every signal,
not just ads CTR:

| segment | mean rides/day |
|---|---|
| commuter | 2.5 |
| traveler | 1.5 |
| nightlife | 1.0 |
| foodie | 1.0 |
| shopper | 1.0 |
| general | 1.0 |
| homebody | 0.3 (suppressed — low-engagement control) |
