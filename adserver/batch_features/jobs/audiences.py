"""audience_memberships — named, versioned audiences evaluated as rules
over registry features.

Self-contained like every other job: recomputes the features it needs
(segment, user_rides_per_week, user_ctr_by_category_30d) directly via the
same shared helpers other jobs use, rather than depending on their output
or run order.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import (
    ctr_by_category,
    load_events,
    load_rides,
    load_users,
    rides_count,
)
from adserver.common.audiences import AudienceDef, Rule, load_audiences
from adserver.datagen.lifts import CATEGORIES

AUDIENCES_PATH = Path(__file__).resolve().parents[2] / "common" / "audiences.yaml"
VERSION_LOG_PATH = Path("data") / "audience_versions.log"

RIDES_WINDOW_DAYS = 7
CTR_WINDOW_DAYS = 30

_OPS = {
    "eq": lambda col, val: col == val,
    "ne": lambda col, val: col != val,
    "gt": lambda col, val: col > val,
    "gte": lambda col, val: col >= val,
    "lt": lambda col, val: col < val,
    "lte": lambda col, val: col <= val,
}


def _rule_expr(rule: Rule) -> pl.Expr:
    return _OPS[rule.op](pl.col(rule.feature), rule.value)


def _feature_context(as_of: dt.date, data_dir: Path) -> pl.DataFrame:
    """One row per user with every column an audience rule might reference,
    column-named to match the rule's `feature` string exactly (including
    the dotted map-lookup form, e.g. 'user_ctr_by_category_30d.travel')."""
    users = load_users(data_dir)
    rides = load_rides(data_dir)
    events = load_events(data_dir)

    rides_df = (
        rides_count(rides, as_of, RIDES_WINDOW_DAYS)
        .select(["user_id", pl.col("rides").cast(pl.Float64).alias("user_rides_per_week")])
    )

    ctr_wide = ctr_by_category(events, as_of, CTR_WINDOW_DAYS).pivot(
        on="category", index="user_id", values="ctr"
    ).fill_null(0.0)
    for category in CATEGORIES:
        if category not in ctr_wide.columns:
            ctr_wide = ctr_wide.with_columns(pl.lit(0.0).alias(category))
    ctr_wide = ctr_wide.rename(
        {c: f"user_ctr_by_category_30d.{c}" for c in CATEGORIES}
    )

    ctx = users.select(["user_id", "segment"]).join(rides_df, on="user_id", how="left")
    ctx = ctx.with_columns(pl.col("user_rides_per_week").fill_null(0.0))
    ctx = ctx.join(ctr_wide, on="user_id", how="left")
    for category in CATEGORIES:
        col = f"user_ctr_by_category_30d.{category}"
        ctx = ctx.with_columns(pl.col(col).fill_null(0.0))
    return ctx


def _log_versions(audiences: dict[str, AudienceDef], as_of: dt.date) -> None:
    VERSION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with VERSION_LOG_PATH.open("a") as f:
        for aud in audiences.values():
            f.write(
                json.dumps(
                    {
                        "name": aud.name,
                        "definition_version": aud.definition_version,
                        "as_of": as_of.isoformat(),
                    }
                )
                + "\n"
            )


class AudienceMembershipsJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["audience_memberships"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        audiences = load_audiences(AUDIENCES_PATH)
        _log_versions(audiences, as_of)

        ctx = _feature_context(as_of, data_dir)

        for aud in audiences.values():
            expr = None
            for rule in aud.rules:
                clause = _rule_expr(rule)
                expr = clause if expr is None else expr & clause
            ctx = ctx.with_columns(expr.alias(f"__match__{aud.name}"))

        names = list(audiences.keys())
        memberships = [
            [name for name in names if row[f"__match__{name}"]]
            for row in ctx.iter_rows(named=True)
        ]

        return ctx.select(["user_id"]).with_columns(
            pl.Series("audience_memberships", memberships, dtype=pl.List(pl.Utf8))
        )
