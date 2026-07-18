"""campaign_spend_yesterday — impressions x bid for the prior day."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.batch_features.jobs._shared import load_campaigns, load_events, spend_yesterday


class CampaignSpendYesterdayJob(FeatureJob):
    def entity(self) -> str:
        return "ad"

    def outputs(self) -> list[str]:
        return ["campaign_spend_yesterday"]

    def compute(self, as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
        events = load_events(data_dir)
        campaigns = load_campaigns(data_dir)
        out = spend_yesterday(events, campaigns, as_of)
        return out.rename({"spend": "campaign_spend_yesterday"})
