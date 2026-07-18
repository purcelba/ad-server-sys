"""Synthetic ad campaign catalog generation."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from adserver.datagen.lifts import CATEGORIES
from adserver.datagen.users import HISTORY_END, HISTORY_START

DEMAND_TYPES = ["auction", "guaranteed"]

CAMPAIGNS_PER_CATEGORY_PER_DEMAND = 4  # 5 categories * 2 demand types * 4 = 40
N_CAMPAIGNS = len(CATEGORIES) * len(DEMAND_TYPES) * CAMPAIGNS_PER_CATEGORY_PER_DEMAND

ADVERTISER_NAMES: dict[str, list[str]] = {
    "food": ["Bite Bistro", "Corner Diner", "Fresh Fork", "Noodle House"],
    "retail": ["Urban Threads", "Home & Co", "The Market Stall", "Bright Basics"],
    "entertainment": ["Cinema Row", "Live Stage Co", "Arcade Loop", "Studio Nightclub"],
    "travel": ["Skyward Air", "Wanderlust Tours", "Harbor Cruises", "Peak Getaways"],
    "transit": ["QuickRail", "MetroLink", "CityBike Share", "Commuter Express"],
}

BID_RANGE = (0.50, 5.00)
BUDGET_RANGE = (200.0, 2000.0)
IMPRESSION_GOAL_RANGE = (500, 5000)
FLIGHT_START_JITTER_DAYS = (-5, 20)  # relative to HISTORY_START
FLIGHT_DURATION_DAYS = (10, 40)


def generate_campaigns(rng: np.random.Generator) -> pl.DataFrame:
    """Generate the campaigns.parquet catalog deterministically from `rng`."""
    rows: list[dict] = []
    campaign_num = 1
    for category in CATEGORIES:
        names = ADVERTISER_NAMES[category]
        for demand_type in DEMAND_TYPES:
            for i in range(CAMPAIGNS_PER_CATEGORY_PER_DEMAND):
                campaign_id = f"c_{campaign_num:04d}"
                advertiser_name = names[i % len(names)]

                start_jitter = int(rng.integers(*FLIGHT_START_JITTER_DAYS))
                duration = int(rng.integers(*FLIGHT_DURATION_DAYS))
                flight_start = HISTORY_START + dt.timedelta(days=start_jitter)
                flight_end = flight_start + dt.timedelta(days=duration)

                if demand_type == "auction":
                    bid = round(float(rng.uniform(*BID_RANGE)), 2)
                    budget = round(float(rng.uniform(*BUDGET_RANGE)), 2)
                    impression_goal = None
                else:
                    bid = None
                    budget = None
                    impression_goal = int(rng.integers(*IMPRESSION_GOAL_RANGE))

                status = "active"
                if flight_end < HISTORY_END:
                    status = "ended"
                elif campaign_num % 8 == 0:
                    status = "paused"

                rows.append(
                    {
                        "campaign_id": campaign_id,
                        "advertiser_name": advertiser_name,
                        "category": category,
                        "demand_type": demand_type,
                        "bid": bid,
                        "budget": budget,
                        "impression_goal": impression_goal,
                        "flight_start": flight_start,
                        "flight_end": flight_end,
                        "status": status,
                    }
                )
                campaign_num += 1

    return pl.DataFrame(
        rows,
        schema={
            "campaign_id": pl.Utf8,
            "advertiser_name": pl.Utf8,
            "category": pl.Utf8,
            "demand_type": pl.Utf8,
            "bid": pl.Float64,
            "budget": pl.Float64,
            "impression_goal": pl.Int64,
            "flight_start": pl.Date,
            "flight_end": pl.Date,
            "status": pl.Utf8,
        },
    ).sort("campaign_id")


def active_campaigns_on(campaigns: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    """Campaigns whose flight covers `day` and whose status is active."""
    return campaigns.filter(
        (pl.col("status") == "active")
        & (pl.col("flight_start") <= day)
        & (pl.col("flight_end") >= day)
    )
