import random

from adserver.ops.readout import cis_overlap, per_arm_ctr, wilson_interval

USERS_BY_ID = {
    "u_low": {"user_id": "u_low", "segment": "homebody"},
    "u_high": {"user_id": "u_high", "segment": "foodie"},
}
CAMPAIGNS_BY_ID = {
    "c_low": {"campaign_id": "c_low", "category": "entertainment"},
    "c_high": {"campaign_id": "c_high", "category": "food"},
}


def _decision(arm, winner, user_id, ts="2026-07-20 12:00:00"):
    return {"experiment_arm": arm, "winner": winner, "user_id": user_id, "ts": ts}


def test_wilson_interval_widens_as_impressions_shrink():
    lo_big, hi_big = wilson_interval(clicks=100, impressions=1000)
    lo_small, hi_small = wilson_interval(clicks=1, impressions=10)
    assert (hi_big - lo_big) < (hi_small - lo_small)


def test_wilson_interval_zero_impressions_is_zero_width():
    assert wilson_interval(0, 0) == (0.0, 0.0)


def test_per_arm_ctr_excludes_no_fill_and_house_ad():
    decisions = [
        _decision("control", None, "u_low"),
        _decision("control", "house_ad", "u_low"),
        _decision("control", "c_low", "u_low"),
    ]
    report = per_arm_ctr(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, random.Random(0))
    assert report["control"]["impressions"] == 1


def test_per_arm_ctr_groups_by_experiment_arm():
    decisions = [_decision("control", "c_low", "u_low") for _ in range(5)] + [
        _decision("treatment", "c_high", "u_high") for _ in range(5)
    ]
    report = per_arm_ctr(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, random.Random(0))
    assert set(report) == {"control", "treatment"}
    assert report["control"]["impressions"] == 5
    assert report["treatment"]["impressions"] == 5


def test_ac3_planted_ctr_difference_is_detected_at_n_5000():
    # control: homebody segment x entertainment category - a 0.3x-lift
    # pairing (datagen/lifts.py), ~0.009 underlying CTR.
    # treatment: foodie segment x food category - a 3.0x-lift pairing,
    # ~0.09 underlying CTR, a real 10x gap. 2,500 impressions per arm
    # (5,000 total) should make that gap easily detectable - proving the
    # readout script's own CI-overlap check works, the same "verify the
    # mechanism" discipline as Phase 5 AC4's pacing race.
    decisions = [_decision("control", "c_low", "u_low") for _ in range(2500)] + [
        _decision("treatment", "c_high", "u_high") for _ in range(2500)
    ]
    report = per_arm_ctr(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, random.Random(42))

    assert report["control"]["impressions"] == 2500
    assert report["treatment"]["impressions"] == 2500
    assert report["treatment"]["ctr"] > report["control"]["ctr"]
    assert not cis_overlap(report, "control", "treatment")
