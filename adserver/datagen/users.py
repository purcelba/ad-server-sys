"""Synthetic user catalog generation."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

# segment -> count. Must sum to N_USERS.
SEGMENT_COUNTS: dict[str, int] = {
    "commuter": 10,
    "traveler": 8,
    "nightlife": 8,
    "foodie": 7,
    "shopper": 7,
    "homebody": 5,
    "general": 5,
}

N_USERS = sum(SEGMENT_COUNTS.values())

METROS = ["san_francisco", "new_york", "chicago", "austin", "seattle"]

HISTORY_START = dt.date(2026, 6, 19)  # 30 days ending HISTORY_END (inclusive)
HISTORY_END = dt.date(2026, 7, 18)


def generate_users(rng: np.random.Generator) -> pl.DataFrame:
    """Generate the users.parquet catalog deterministically from `rng`."""
    segments: list[str] = []
    for segment, count in SEGMENT_COUNTS.items():
        segments.extend([segment] * count)

    user_ids = [f"u_{i:04d}" for i in range(1, N_USERS + 1)]
    home_metros = [METROS[i] for i in rng.integers(0, len(METROS), size=N_USERS)]

    # account creation: uniformly within [history_start - 180d, history_end]
    window_days = (HISTORY_END - (HISTORY_START - dt.timedelta(days=180))).days
    offsets = rng.integers(0, window_days + 1, size=N_USERS)
    created_ats = [
        HISTORY_START - dt.timedelta(days=180) + dt.timedelta(days=int(o)) for o in offsets
    ]

    return pl.DataFrame(
        {
            "user_id": user_ids,
            "segment": segments,
            "home_metro": home_metros,
            "created_at": created_ats,
        }
    ).sort("user_id")
