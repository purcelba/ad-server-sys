# batch_features

## What it does
Computes the registry's batch features from history and writes
date-partitioned offline Parquet output, point-in-time correct (an
`as_of` date only ever sees data up to and including that date).

**Feature-job interface** (`framework.py`): every job implements
`entity()` (`"user"`|`"ad"`), `outputs()` (registry feature names), and
`compute(as_of, data_dir) -> pl.DataFrame` (one row per entity, id column +
one column per output). Jobs are self-contained — each reads raw parquet
directly, no dependency on another job's output or run order.

**Runner** (`runner.py`): auto-discovers every `FeatureJob` subclass under
`jobs/` (no hand-maintained job list — this is the seam an extension plugs
into: a new feature family is one new module + registry entries, zero
edits here), validates each job's output columns against `outputs()` and
the registry, joins per-entity outputs into one wide frame, and writes
`data/features/entity=<user|ad>/asof=<date>/features.parquet`.

Currently implemented jobs (all 9 registry features): `user_ctr_by_category_30d`,
`user_ctr_30d`, `user_impressions_7d`, `user_rides_per_week`, `ad_ctr_7d`,
`ad_ctr_30d`, `ad_impressions_7d`, `campaign_spend_yesterday`, and
`audience_memberships` (`jobs/audiences.py` — evaluates `common/audiences.yaml`
rules against the same shared helpers the other jobs use, self-contained
like every other job, not dependent on their output or run order). Shared
pure computation helpers (window filtering, CTR, impression counts, spend)
live in `jobs/_shared.py` so logic isn't duplicated across jobs.

**Reach report** (`reach.py`, `make reach`): member counts and pairwise
overlap per audience — "how many riders would this campaign reach?"

**Data-quality gate** (`quality.py`): the runner calls `check()` on every
job's output before it's eligible for materialization — a single failure
raises `QualityGateError` and aborts the whole run before anything is
written (no partial output). Two checks: (1) row-count — fewer than 80% of
the job's *expected entity population* represented; (2) null-rate — more
than 5% of a feature column's values are null. The row-count denominator
is job-specific, not always "every entity in the catalog":
`FeatureJob.expected_entity_count()` defaults to the full catalog (correct
for user-level jobs, given this project's data volume gives near-universal
weekly coverage), but ad-level windowed jobs (`ad_ctr_7d`/`30d`,
`ad_impressions_7d`) override it to count only campaigns whose flight is
`active` and overlaps the window — a campaign outside its flight has no
impressions *by design*, not by data corruption, and conflating the two
would make the gate fire on entirely correct output (verified directly:
22/40 campaigns get impressions in a given 7-day window, but that's 22/22
of the campaigns actually eligible in that window).

(Materialization to DynamoDB-local is planned for this package within
Phase 1; not yet present as of this commit.)

## How to run and test it alone
```bash
uv run pytest adserver/batch_features -v
```
No infra dependency for the jobs/runner themselves — pure Python + Polars
over the `datagen/`-generated parquet files. (Materialization, once added,
will need `dynamodb-local` up via `make up`.)

## Production analog
This is the local, file-based stand-in for a production batch feature
pipeline — e.g. Spark/Airflow jobs writing to a data warehouse, with
results synced to an online feature store (DynamoDB/Redis/Feast-style
online store). The auto-discovery + registry-validation pattern mirrors
how real feature platforms let many teams add features independently
without a central team reviewing every pipeline change.

## Ownership note
Under an end-to-end ads team model, the *jobs themselves* (which features
to compute) are plausibly owned by the ads team — they know what signal
ranking needs. The *runner/framework* (the shared contract and discovery
mechanism) is more plausibly owned by a central ML/data platform team,
since its whole value is consistency across many teams' jobs. The
tradeoff: ads-team ownership of the framework moves faster short-term but
risks per-team drift; platform ownership is slower to change but keeps the
seam clean for everyone.
