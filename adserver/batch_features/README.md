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

Currently implemented jobs (8 of the registry's 9 features —
`audience_memberships` lands in a later step): `user_ctr_by_category_30d`,
`user_ctr_30d`, `user_impressions_7d`, `user_rides_per_week`, `ad_ctr_7d`,
`ad_ctr_30d`, `ad_impressions_7d`, `campaign_spend_yesterday`. Shared pure
computation helpers (window filtering, CTR, impression counts, spend) live
in `jobs/_shared.py` so logic isn't duplicated across jobs.

(Data-quality gate and materialization to DynamoDB-local are planned for
this package within Phase 1; not yet present as of this commit.)

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
