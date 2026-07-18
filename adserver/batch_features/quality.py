"""Data-quality gate: row-count and null-rate checks. Failures block
materialization entirely — no partial writes.

Policy:
- row-count: fewer than MIN_ROW_COUNT_RATIO of the job's expected entity
  population represented -> fail. The denominator is job-specific
  (`FeatureJob.expected_entity_count()`), not always "every entity in the
  catalog" — some features (e.g. ad-level windowed CTR) are legitimately
  sparse for entities with no data in the window, which is not the same
  thing as corruption.
- null-rate: more than MAX_NULL_RATE of values across the job's own
  feature columns (within the rows it did produce) are null -> fail.
"""

from __future__ import annotations

import polars as pl

MIN_ROW_COUNT_RATIO = 0.8
MAX_NULL_RATE = 0.05


class QualityGateError(ValueError):
    pass


def check(
    df: pl.DataFrame, expected_entity_count: int, feature_cols: list[str], job_name: str
) -> None:
    if expected_entity_count > 0:
        ratio = df.height / expected_entity_count
        if ratio < MIN_ROW_COUNT_RATIO:
            raise QualityGateError(
                f"{job_name}: row-count gate failed — {df.height}/{expected_entity_count} "
                f"entities represented ({ratio:.1%} < {MIN_ROW_COUNT_RATIO:.0%} threshold)"
            )

    for col in feature_cols:
        total = df.height
        if total == 0:
            continue
        null_count = df[col].null_count()
        null_rate = null_count / total
        if null_rate > MAX_NULL_RATE:
            raise QualityGateError(
                f"{job_name}: null-rate gate failed on {col!r} — {null_count}/{total} "
                f"null ({null_rate:.1%} > {MAX_NULL_RATE:.0%} threshold)"
            )
