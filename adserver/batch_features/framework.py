"""Feature-job interface — the extensibility contract batch jobs implement.

Every job is self-contained: `compute()` reads directly from the raw
datagen parquet files for a given as-of date, with no dependency on other
jobs' output or run order. Adding a new feature family means adding one
job module in `batch_features/jobs/` (+ registry entries) — the runner
auto-discovers it, zero edits required here or in `runner.py`.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

DEFAULT_DATA_DIR = Path("data")


class FeatureJob(ABC):
    @abstractmethod
    def entity(self) -> str:
        """'user' or 'ad' — every output column belongs to one entity type."""

    @abstractmethod
    def outputs(self) -> list[str]:
        """Registry feature names this job produces."""

    @abstractmethod
    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        """Compute this job's feature(s) as of `as_of` (inclusive).

        Returns one row per entity, with an entity id column
        (`user_id` or `campaign_id`, matching `entity()`) plus one column
        per name in `outputs()`. Reads raw parquet directly from
        `data_dir` — never touches another job's output.
        """

    def entity_id_column(self) -> str:
        return "user_id" if self.entity() == "user" else "campaign_id"
