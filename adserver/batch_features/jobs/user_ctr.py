"""user_ctr_30d — overall user CTR across all categories."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import ctr_overall, load_events

WINDOW_DAYS = 30


class UserCtrJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_ctr_30d"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        out = ctr_overall(events, as_of, WINDOW_DAYS, "user_id")
        return out.rename({"ctr": "user_ctr_30d"})
