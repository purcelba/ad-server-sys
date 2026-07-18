import numpy as np

from adserver.datagen.rides import RIDE_TYPES, generate_rides
from adserver.datagen.users import generate_users


def _rides(seed=1):
    rng = np.random.default_rng(seed)
    users = generate_users(rng)
    return users, generate_rides(rng, users)


def test_schema_and_fk_integrity():
    users, rides = _rides()
    assert rides.columns == ["ride_id", "user_id", "ts", "ride_date", "ride_type"]
    assert rides["ride_id"].n_unique() == rides.height
    assert set(rides["user_id"].unique().to_list()).issubset(set(users["user_id"].to_list()))
    assert set(rides["ride_type"].unique().to_list()).issubset(set(RIDE_TYPES))


def test_ride_date_derived_from_ts():
    _, rides = _rides()
    assert (rides["ts"].dt.date() == rides["ride_date"]).all()


def test_commuter_rides_more_than_general_and_homebody_less():
    """Statistical smoke test: the segment-dependent ride rate is actually
    present in generated output, not just the RIDES_PER_DAY_MEAN constants."""
    import polars as pl

    users, rides = _rides(seed=42)
    counts = rides.group_by("user_id").len().join(
        users.select(["user_id", "segment"]), on="user_id", how="right"
    ).fill_null(0)
    avg_by_segment = counts.group_by("segment").agg(pl.col("len").mean().alias("avg_rides"))

    def avg_for(segment):
        return avg_by_segment.filter(avg_by_segment["segment"] == segment)["avg_rides"].item()

    assert avg_for("commuter") > avg_for("general")
    assert avg_for("homebody") < avg_for("general")
