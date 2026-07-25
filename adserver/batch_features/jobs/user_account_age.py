"""user_account_age_days — days since account creation.

Added as Phase 1's AC5 extensibility proof: this module + one registry
entry is the entire engineering change. No edits to runner.py,
framework.py, quality.py, or materialize.py.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import load_users


class UserAccountAgeJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_account_age_days"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        users = load_users(data_dir)
        age = (pl.lit(as_of) - pl.col("created_at")).dt.total_days()
        return users.select(
            ["user_id", age.cast(pl.Int64).alias("user_account_age_days")]
        )
