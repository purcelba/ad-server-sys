"""Synthetic impression/click event log generation with planted CTR lift."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from adserver.datagen.campaigns import active_campaigns_on
from adserver.datagen.lifts import click_probability
from adserver.datagen.users import HISTORY_END, HISTORY_START

IMPRESSIONS_PER_USER_PER_DAY_MEAN = 10.0
CLICK_DELAY_SECONDS_RANGE = (1, 120)


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    days = (end - start).days
    return [start + dt.timedelta(days=i) for i in range(days + 1)]


def generate_events(
    rng: np.random.Generator, users: pl.DataFrame, campaigns: pl.DataFrame
) -> pl.DataFrame:
    """Generate events.parquet deterministically from `rng`.

    Iteration order (sorted user_id -> sorted day -> impression index) is
    fixed so the same seed always produces the same sequence of RNG draws,
    which is what makes byte-identical output possible.
    """
    user_rows = users.sort("user_id").to_dicts()
    campaigns_by_day: dict[dt.date, pl.DataFrame] = {
        day: active_campaigns_on(campaigns, day)
        for day in _date_range(HISTORY_START, HISTORY_END)
    }

    rows: list[dict] = []
    event_num = 1

    for user in user_rows:
        user_id = user["user_id"]
        segment = user["segment"]
        for day in _date_range(HISTORY_START, HISTORY_END):
            eligible = campaigns_by_day[day]
            if eligible.height == 0:
                continue

            n_impressions = int(rng.poisson(IMPRESSIONS_PER_USER_PER_DAY_MEAN))
            eligible_ids = eligible["campaign_id"].to_list()
            eligible_categories = eligible["category"].to_list()

            for _ in range(n_impressions):
                idx = int(rng.integers(0, len(eligible_ids)))
                campaign_id = eligible_ids[idx]
                category = eligible_categories[idx]

                hour = int(rng.integers(0, 24))
                minute = int(rng.integers(0, 60))
                second = int(rng.integers(0, 60))
                ts = dt.datetime.combine(day, dt.time(hour, minute, second))

                impression_id = f"e_{event_num:08d}"
                event_num += 1
                rows.append(
                    {
                        "event_id": impression_id,
                        "event_type": "impression",
                        "user_id": user_id,
                        "campaign_id": campaign_id,
                        "category": category,
                        "segment": segment,
                        "ts": ts,
                        "event_date": day,
                        "hour_of_day": hour,
                        "click_id": None,
                    }
                )

                p_click = click_probability(segment, category, hour)
                if rng.uniform(0.0, 1.0) < p_click:
                    delay = int(rng.integers(*CLICK_DELAY_SECONDS_RANGE))
                    click_ts = ts + dt.timedelta(seconds=delay)
                    click_id = f"e_{event_num:08d}"
                    event_num += 1
                    rows.append(
                        {
                            "event_id": click_id,
                            "event_type": "click",
                            "user_id": user_id,
                            "campaign_id": campaign_id,
                            "category": category,
                            "segment": segment,
                            "ts": click_ts,
                            "event_date": click_ts.date(),
                            "hour_of_day": click_ts.hour,
                            "click_id": impression_id,
                        }
                    )

    return pl.DataFrame(
        rows,
        schema={
            "event_id": pl.Utf8,
            "event_type": pl.Utf8,
            "user_id": pl.Utf8,
            "campaign_id": pl.Utf8,
            "category": pl.Utf8,
            "segment": pl.Utf8,
            "ts": pl.Datetime(time_unit="us"),
            "event_date": pl.Date,
            "hour_of_day": pl.Int64,
            "click_id": pl.Utf8,
        },
    ).sort("event_id")
