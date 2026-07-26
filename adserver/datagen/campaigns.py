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


# Phase 5 amendment (flagged, phase-0 tag not moved — see PROGRESS.md):
# campaign audience targeting didn't exist anywhere in the data model.
# audiences.yaml/audience_memberships (Phase 1) say which *users* belong
# to an audience; nothing said which *campaigns* purchased targeting on
# one, which Phase 5's audience routing rule (retrieval must exclude a
# campaign from users outside its targeted audience) needs. Exactly two
# campaigns are targeted, deterministically and thematically (mirroring
# lifts.py's own segment x category pairing): the first active `travel`
# campaign targets `frequent_airport_travelers`, the first active
# `transit` campaign targets `weekday_commuters`. Every other campaign is
# untargeted (empty list) — unaffected by audience membership, per the
# spec's own "eligibility only, and only when purchased" rule.
AUDIENCE_TARGETING_BY_CATEGORY = {
    "travel": "frequent_airport_travelers",
    "transit": "weekday_commuters",
}


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
                        "targeted_audiences": [],
                    }
                )
                campaign_num += 1

    remaining_targeting = dict(AUDIENCE_TARGETING_BY_CATEGORY)  # local copy - never mutate the module constant
    for row in rows:
        if not remaining_targeting:
            break
        if row["category"] not in remaining_targeting or row["status"] != "active":
            continue
        row["targeted_audiences"] = [remaining_targeting.pop(row["category"])]

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
            "targeted_audiences": pl.List(pl.Utf8),
        },
    ).sort("campaign_id")


def active_campaigns_on(campaigns: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    """Campaigns whose flight covers `day` and whose status is active."""
    return campaigns.filter(
        (pl.col("status") == "active")
        & (pl.col("flight_start") <= day)
        & (pl.col("flight_end") >= day)
    )
