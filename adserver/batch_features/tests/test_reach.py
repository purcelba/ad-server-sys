import datetime as dt

from adserver.batch_features.reach import compute_reach

AS_OF = dt.date(2026, 7, 18)


def test_counts_match_segment_sizes():
    report = compute_reach(AS_OF)
    assert report["total_users"] == 50
    # every commuter/traveler matches their audience given current thresholds
    assert report["counts"]["weekday_commuters"] == 10
    assert report["counts"]["frequent_airport_travelers"] == 8


def test_mutually_exclusive_audiences_have_zero_overlap():
    report = compute_reach(AS_OF)
    key = ("frequent_airport_travelers", "weekday_commuters")
    assert report["overlaps"][key] == 0
