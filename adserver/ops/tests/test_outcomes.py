import datetime as dt
import random

from adserver.ops.outcomes import simulate_click


def _decision(**overrides):
    base = {
        "winner": "c_0001",
        "user_id": "u_0001",
        "ts": dt.datetime(2026, 7, 20, 12, 0),
    }
    base.update(overrides)
    return base


USERS = {"u_0001": {"user_id": "u_0001", "segment": "foodie"}}
CAMPAIGNS = {"c_0001": {"campaign_id": "c_0001", "category": "food"}}


def test_no_winner_never_clicks():
    rng = random.Random(0)
    assert simulate_click(_decision(winner=None), USERS, CAMPAIGNS, rng) is False


def test_house_ad_never_clicks():
    rng = random.Random(0)
    assert simulate_click(_decision(winner="house_ad"), USERS, CAMPAIGNS, rng) is False


def test_string_timestamp_is_parsed_same_as_datetime():
    decision_dt = _decision()
    decision_str = _decision(ts="2026-07-20 12:00:00")
    assert simulate_click(decision_dt, USERS, CAMPAIGNS, random.Random(7)) == simulate_click(
        decision_str, USERS, CAMPAIGNS, random.Random(7)
    )


def test_high_lift_segment_category_clicks_much_more_often_than_baseline():
    # foodie x food is a 3.0x lift (planted in datagen/lifts.py) - over
    # many draws with a fixed seed, the click rate should track ~0.09
    # (0.03 base x 3.0), not the 0.03 baseline for an unrelated pairing.
    rng = random.Random(42)
    clicks = sum(simulate_click(_decision(), USERS, CAMPAIGNS, rng) for _ in range(20_000))
    rate = clicks / 20_000
    assert 0.08 < rate < 0.10


def test_unknown_user_or_campaign_never_clicks():
    rng = random.Random(0)
    assert simulate_click(_decision(user_id="ghost"), USERS, CAMPAIGNS, rng) is False
    assert simulate_click(_decision(winner="ghost"), USERS, CAMPAIGNS, rng) is False
