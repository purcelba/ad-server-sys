"""ad_ctr_7d and ad_ctr_30d — campaign-level CTR at two windows."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import (
    ctr_overall,
    eligible_campaigns_count,
    load_campaigns,
    load_events,
)


class AdCtr7dJob(FeatureJob):
    WINDOW_DAYS = 7

    def entity(self) -> str:
        return "ad"

    def outputs(self) -> list[str]:
        return ["ad_ctr_7d"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        out = ctr_overall(events, as_of, self.WINDOW_DAYS, "campaign_id")
        return out.rename({"ctr": "ad_ctr_7d"})

    def expected_entity_count(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> int:
        campaigns = load_campaigns(data_dir)
        return eligible_campaigns_count(campaigns, as_of, self.WINDOW_DAYS)


class AdCtr30dJob(FeatureJob):
    WINDOW_DAYS = 30

    def entity(self) -> str:
        return "ad"

    def outputs(self) -> list[str]:
        return ["ad_ctr_30d"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        out = ctr_overall(events, as_of, self.WINDOW_DAYS, "campaign_id")
        return out.rename({"ctr": "ad_ctr_30d"})

    def expected_entity_count(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> int:
        campaigns = load_campaigns(data_dir)
        return eligible_campaigns_count(campaigns, as_of, self.WINDOW_DAYS)
