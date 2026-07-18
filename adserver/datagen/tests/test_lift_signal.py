"""Statistical smoke test: the generated events actually carry the planted
signal from lifts.py, not just the constant existing in code."""

import numpy as np
import polars as pl

from adserver.datagen.campaigns import generate_campaigns
from adserver.datagen.events import generate_events
from adserver.datagen.users import generate_users


def _ctr_table(seed=42) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    users = generate_users(rng)
    campaigns = generate_campaigns(rng)
    events = generate_events(rng, users, campaigns)

    impressions = events.filter(events["event_type"] == "impression")
    clicks = events.filter(events["event_type"] == "click")

    imp_counts = impressions.group_by(["segment", "category"]).len().rename({"len": "impressions"})
    click_counts = clicks.group_by(["segment", "category"]).len().rename({"len": "clicks"})

    ctr = imp_counts.join(click_counts, on=["segment", "category"], how="left").fill_null(0)
    ctr = ctr.with_columns((pl.col("clicks") / pl.col("impressions")).alias("ctr"))
    return ctr


def _ctr_for(ctr: pl.DataFrame, segment: str, category: str) -> float:
    row = ctr.filter((ctr["segment"] == segment) & (ctr["category"] == category))
    if row.height == 0:
        return 0.0
    return row["ctr"].item()


def test_traveler_travel_ctr_beats_traveler_other_and_general_travel():
    ctr = _ctr_table()

    traveler_travel = _ctr_for(ctr, "traveler", "travel")
    traveler_other = ctr.filter((ctr["segment"] == "traveler") & (ctr["category"] != "travel"))["ctr"].mean()
    general_travel = _ctr_for(ctr, "general", "travel")

    assert traveler_travel > traveler_other
    assert traveler_travel > general_travel


def test_foodie_food_ctr_beats_general_food():
    ctr = _ctr_table()
    assert _ctr_for(ctr, "foodie", "food") > _ctr_for(ctr, "general", "food")


def test_homebody_ctr_suppressed_relative_to_general():
    ctr = _ctr_table()
    homebody_overall = ctr.filter(ctr["segment"] == "homebody")["ctr"].mean()
    general_overall = ctr.filter(ctr["segment"] == "general")["ctr"].mean()
    assert homebody_overall < general_overall
