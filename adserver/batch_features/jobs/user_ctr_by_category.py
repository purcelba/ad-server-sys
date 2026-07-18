"""user_ctr_by_category_30d — per-category CTR, map[str,float]."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import ctr_by_category, load_events
from adserver.datagen.lifts import CATEGORIES

WINDOW_DAYS = 30


class UserCtrByCategoryJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_ctr_by_category_30d"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        long = ctr_by_category(events, as_of, WINDOW_DAYS)

        wide = long.pivot(on="category", index="user_id", values="ctr").fill_null(0.0)
        for category in CATEGORIES:
            if category not in wide.columns:
                wide = wide.with_columns(pl.lit(0.0).alias(category))

        return wide.select(
            ["user_id", pl.struct(CATEGORIES).alias("user_ctr_by_category_30d")]
        )
