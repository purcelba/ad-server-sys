import numpy as np

from adserver.datagen.campaigns import generate_campaigns
from adserver.datagen.events import generate_events
from adserver.datagen.users import generate_users


def _generate(seed=1):
    rng = np.random.default_rng(seed)
    users = generate_users(rng)
    campaigns = generate_campaigns(rng)
    events = generate_events(rng, users, campaigns)
    return users, campaigns, events


def test_schema_and_event_types():
    _, _, events = _generate()
    assert events.columns == [
        "event_id",
        "event_type",
        "user_id",
        "campaign_id",
        "category",
        "segment",
        "ts",
        "event_date",
        "hour_of_day",
        "click_id",
    ]
    assert set(events["event_type"].unique().to_list()) == {"impression", "click"}
    assert events["event_id"].n_unique() == events.height


def test_click_fk_integrity():
    _, _, events = _generate()
    impressions = events.filter(events["event_type"] == "impression").select(
        ["event_id", "ts"]
    ).rename({"event_id": "click_id", "ts": "impression_ts"})
    clicks = events.filter(events["event_type"] == "click")

    joined = clicks.join(impressions, on="click_id", how="left")
    assert joined["impression_ts"].null_count() == 0, "every click must reference a real impression"
    assert (joined["ts"] > joined["impression_ts"]).all(), "click must occur after its impression"


def test_event_date_derived_from_ts():
    _, _, events = _generate()
    derived = events["ts"].dt.date()
    assert (derived == events["event_date"]).all()


def test_hour_of_day_range():
    _, _, events = _generate()
    assert events["hour_of_day"].min() >= 0
    assert events["hour_of_day"].max() <= 23
