"""Synthetic ride history generation.

Added during Phase 1 planning (flagged amendment to the tagged Phase 0
datagen — see PROGRESS.md): Phase 1's `user_rides_per_week` batch feature
needs a real data source distinct from ad impression/click events, per the
real-time-vs-batch-store discussion in CLAUDE.md. Rides are a separate
table, not folded into events.parquet, since they have no campaign/ad
dimension at all.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from adserver.datagen.users import HISTORY_END, HISTORY_START

# segment -> mean rides/user/day. Mirrors the ads lift design: commuters
# ride most, homebody stays a low-engagement control across all signals,
# not just ads CTR.
RIDES_PER_DAY_MEAN: dict[str, float] = {
    "commuter": 2.5,
    "traveler": 1.5,
    "nightlife": 1.0,
    "foodie": 1.0,
    "shopper": 1.0,
    "general": 1.0,
    "homebody": 0.3,
}

RIDE_TYPES = ["standard", "shared", "premium"]


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    days = (end - start).days
    return [start + dt.timedelta(days=i) for i in range(days + 1)]


def generate_rides(rng: np.random.Generator, users: pl.DataFrame) -> pl.DataFrame:
    """Generate rides.parquet deterministically from `rng`.

    Iteration order (sorted user_id -> sorted day -> ride index) is fixed,
    same determinism discipline as generate_events.
    """
    user_rows = users.sort("user_id").to_dicts()

    rows: list[dict] = []
    ride_num = 1

    for user in user_rows:
        user_id = user["user_id"]
        segment = user["segment"]
        mean_rides = RIDES_PER_DAY_MEAN[segment]

        for day in _date_range(HISTORY_START, HISTORY_END):
            n_rides = int(rng.poisson(mean_rides))
            for _ in range(n_rides):
                hour = int(rng.integers(0, 24))
                minute = int(rng.integers(0, 60))
                second = int(rng.integers(0, 60))
                ts = dt.datetime.combine(day, dt.time(hour, minute, second))
                ride_type = RIDE_TYPES[int(rng.integers(0, len(RIDE_TYPES)))]

                rows.append(
                    {
                        "ride_id": f"r_{ride_num:08d}",
                        "user_id": user_id,
                        "ts": ts,
                        "ride_date": day,
                        "ride_type": ride_type,
                    }
                )
                ride_num += 1

    return pl.DataFrame(
        rows,
        schema={
            "ride_id": pl.Utf8,
            "user_id": pl.Utf8,
            "ts": pl.Datetime(time_unit="us"),
            "ride_date": pl.Date,
            "ride_type": pl.Utf8,
        },
    ).sort("ride_id")
