"""user_rides_per_week — ride count in the trailing 7 days."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import load_rides, rides_count

WINDOW_DAYS = 7


class UserRidesPerWeekJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_rides_per_week"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        rides = load_rides(data_dir)
        counts = rides_count(rides, as_of, WINDOW_DAYS)
        return counts.select(
            ["user_id", pl.col("rides").cast(pl.Float64).alias("user_rides_per_week")]
        )
