"""Pure computation helpers shared across jobs (and, later, the audience
job) — kept dependency-free of run order: every job reads raw parquet and
computes independently, never another job's output.

Point-in-time convention: `as_of` is the last date whose data is known.
A trailing W-day window means [as_of - W + 1, as_of], inclusive both ends.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from adserver.batch_features.framework import DEFAULT_DATA_DIR


def trailing_window(as_of: dt.date, window_days: int) -> tuple[dt.date, dt.date]:
    return as_of - dt.timedelta(days=window_days - 1), as_of


def load_events(data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
    return pl.read_parquet(data_dir / "events.parquet")


def load_rides(data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
    return pl.read_parquet(data_dir / "rides.parquet")


def load_users(data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
    return pl.read_parquet(data_dir / "users.parquet")


def load_campaigns(data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
    return pl.read_parquet(data_dir / "campaigns.parquet")


def eligible_campaigns_count(campaigns: pl.DataFrame, as_of: dt.date, window_days: int) -> int:
    """Count of campaigns that could plausibly have data in the trailing
    window: status == 'active' and flight overlaps [window_start, as_of].

    Used as the row-count quality-gate denominator for ad-level windowed
    features — campaigns outside their flight or paused/ended have no
    impressions by design, not by data-quality failure, so they must not
    count against coverage.
    """
    start, end = trailing_window(as_of, window_days)
    eligible = campaigns.filter(
        (pl.col("status") == "active")
        & (pl.col("flight_start") <= end)
        & (pl.col("flight_end") >= start)
    )
    return eligible.height


def _events_in_window(events: pl.DataFrame, as_of: dt.date, window_days: int) -> pl.DataFrame:
    start, end = trailing_window(as_of, window_days)
    return events.filter((pl.col("event_date") >= start) & (pl.col("event_date") <= end))


def ctr_overall(
    events: pl.DataFrame, as_of: dt.date, window_days: int, group_col: str
) -> pl.DataFrame:
    """(group_col, ctr) — overall click-through rate over the trailing window."""
    windowed = _events_in_window(events, as_of, window_days)
    impressions = windowed.filter(pl.col("event_type") == "impression")
    clicks = windowed.filter(pl.col("event_type") == "click")

    imp_counts = impressions.group_by(group_col).len().rename({"len": "impressions"})
    click_counts = clicks.group_by(group_col).len().rename({"len": "clicks"})

    out = imp_counts.join(click_counts, on=group_col, how="left").fill_null(0)
    return out.with_columns((pl.col("clicks") / pl.col("impressions")).alias("ctr")).select(
        [group_col, "ctr"]
    )


def ctr_by_category(events: pl.DataFrame, as_of: dt.date, window_days: int) -> pl.DataFrame:
    """(user_id, category, ctr) — per-category CTR over the trailing window."""
    windowed = _events_in_window(events, as_of, window_days)
    impressions = windowed.filter(pl.col("event_type") == "impression")
    clicks = windowed.filter(pl.col("event_type") == "click")

    imp_counts = impressions.group_by(["user_id", "category"]).len().rename({"len": "impressions"})
    click_counts = clicks.group_by(["user_id", "category"]).len().rename({"len": "clicks"})

    out = imp_counts.join(click_counts, on=["user_id", "category"], how="left").fill_null(0)
    return out.with_columns((pl.col("clicks") / pl.col("impressions")).alias("ctr")).select(
        ["user_id", "category", "ctr"]
    )


def impressions_count(
    events: pl.DataFrame, as_of: dt.date, window_days: int, group_col: str
) -> pl.DataFrame:
    """(group_col, impressions) — impression count over the trailing window."""
    windowed = _events_in_window(events, as_of, window_days)
    impressions = windowed.filter(pl.col("event_type") == "impression")
    return impressions.group_by(group_col).len().rename({"len": "impressions"})


def rides_count(rides: pl.DataFrame, as_of: dt.date, window_days: int) -> pl.DataFrame:
    """(user_id, rides) — ride count over the trailing window."""
    start, end = trailing_window(as_of, window_days)
    windowed = rides.filter((pl.col("ride_date") >= start) & (pl.col("ride_date") <= end))
    return windowed.group_by("user_id").len().rename({"len": "rides"})


def spend_yesterday(
    events: pl.DataFrame, campaigns: pl.DataFrame, as_of: dt.date
) -> pl.DataFrame:
    """(campaign_id, spend) for the single day before as_of.

    Auction campaigns: impressions x bid. Guaranteed campaigns have no
    bid, so spend is always 0.0.
    """
    yesterday = as_of - dt.timedelta(days=1)
    impressions = events.filter(
        (pl.col("event_type") == "impression") & (pl.col("event_date") == yesterday)
    )
    imp_counts = impressions.group_by("campaign_id").len().rename({"len": "impressions"})

    auction = campaigns.filter(pl.col("demand_type") == "auction").select(
        ["campaign_id", "bid"]
    )
    spend = auction.join(imp_counts, on="campaign_id", how="left").fill_null(0)
    spend = spend.with_columns((pl.col("impressions") * pl.col("bid")).alias("spend"))

    guaranteed_ids = campaigns.filter(pl.col("demand_type") == "guaranteed")["campaign_id"]
    guaranteed = pl.DataFrame({"campaign_id": guaranteed_ids, "spend": [0.0] * len(guaranteed_ids)})

    return pl.concat([spend.select(["campaign_id", "spend"]), guaranteed])
