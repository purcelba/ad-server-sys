"""user_impressions_7d — ad impression volume, trailing 7 days."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import impressions_count, load_events

WINDOW_DAYS = 7


class UserImpressionsJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_impressions_7d"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        out = impressions_count(events, as_of, WINDOW_DAYS, "user_id")
        return out.select(
            ["user_id", pl.col("impressions").cast(pl.Int64).alias("user_impressions_7d")]
        )
