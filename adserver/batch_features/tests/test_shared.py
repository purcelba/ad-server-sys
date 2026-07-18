import datetime as dt

import polars as pl

from adserver.batch_features.jobs._shared import (
    ctr_by_category,
    ctr_overall,
    impressions_count,
    rides_count,
    spend_yesterday,
    trailing_window,
)


def test_trailing_window():
    start, end = trailing_window(dt.date(2026, 7, 18), 7)
    assert start == dt.date(2026, 7, 12)
    assert end == dt.date(2026, 7, 18)


def _events(rows):
    return pl.DataFrame(
        rows,
        schema={
            "event_type": pl.Utf8,
            "user_id": pl.Utf8,
            "campaign_id": pl.Utf8,
            "category": pl.Utf8,
            "event_date": pl.Date,
        },
    )


def test_ctr_overall_excludes_outside_window():
    events = _events(
        [
            # within the 7d window ending 7/18 (starts 7/12)
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 15)},
            {"event_type": "click", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 15)},
            # outside the 7d window ending 7/18 (starts 7/12) - must be excluded
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 1)},
        ]
    )
    out = ctr_overall(events, dt.date(2026, 7, 18), 7, "user_id")
    row = out.filter(out["user_id"] == "u1")
    assert row["ctr"].item() == 1.0  # 1 impression, 1 click, both within window


def test_ctr_by_category_splits_by_category():
    events = _events(
        [
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 18)},
            {"event_type": "click", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 18)},
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c2", "category": "travel", "event_date": dt.date(2026, 7, 18)},
        ]
    )
    out = ctr_by_category(events, dt.date(2026, 7, 18), 30)
    food = out.filter((out["user_id"] == "u1") & (out["category"] == "food"))
    travel = out.filter((out["user_id"] == "u1") & (out["category"] == "travel"))
    assert food["ctr"].item() == 1.0
    assert travel["ctr"].item() == 0.0


def test_impressions_count():
    events = _events(
        [
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 18)},
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 17)},
            {"event_type": "click", "user_id": "u1", "campaign_id": "c1", "category": "food", "event_date": dt.date(2026, 7, 18)},
        ]
    )
    out = impressions_count(events, dt.date(2026, 7, 18), 7, "user_id")
    assert out.filter(out["user_id"] == "u1")["impressions"].item() == 2


def test_rides_count_excludes_outside_window():
    rides = pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u1"],
            "ride_date": [dt.date(2026, 7, 18), dt.date(2026, 7, 15), dt.date(2026, 7, 1)],
        },
        schema={"user_id": pl.Utf8, "ride_date": pl.Date},
    )
    out = rides_count(rides, dt.date(2026, 7, 18), 7)
    assert out.filter(out["user_id"] == "u1")["rides"].item() == 2


def test_spend_yesterday_auction_vs_guaranteed():
    events = _events(
        [
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c_auction", "category": "food", "event_date": dt.date(2026, 7, 17)},
            {"event_type": "impression", "user_id": "u2", "campaign_id": "c_auction", "category": "food", "event_date": dt.date(2026, 7, 17)},
            # not yesterday - excluded
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c_auction", "category": "food", "event_date": dt.date(2026, 7, 18)},
            {"event_type": "impression", "user_id": "u1", "campaign_id": "c_guaranteed", "category": "food", "event_date": dt.date(2026, 7, 17)},
        ]
    )
    campaigns = pl.DataFrame(
        {
            "campaign_id": ["c_auction", "c_guaranteed"],
            "demand_type": ["auction", "guaranteed"],
            "bid": [2.0, None],
        },
        schema={"campaign_id": pl.Utf8, "demand_type": pl.Utf8, "bid": pl.Float64},
    )
    out = spend_yesterday(events, campaigns, dt.date(2026, 7, 18))
    auction_spend = out.filter(out["campaign_id"] == "c_auction")["spend"].item()
    guaranteed_spend = out.filter(out["campaign_id"] == "c_guaranteed")["spend"].item()
    assert auction_spend == 4.0  # 2 impressions x $2.0 bid
    assert guaranteed_spend == 0.0
