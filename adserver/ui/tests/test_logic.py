from adserver.ui.logic import campaign_label, compute_rate, find_decision


def test_find_decision_returns_matching_entry():
    decisions = [{"request_id": "r1", "winner": "c_0001"}, {"request_id": "r2", "winner": "c_0002"}]
    assert find_decision(decisions, "r2") == {"request_id": "r2", "winner": "c_0002"}


def test_find_decision_returns_none_when_missing():
    decisions = [{"request_id": "r1", "winner": "c_0001"}]
    assert find_decision(decisions, "does_not_exist") is None


def test_find_decision_prefers_most_recent_match():
    decisions = [{"request_id": "r1", "winner": "old"}, {"request_id": "r1", "winner": "new"}]
    assert find_decision(decisions, "r1")["winner"] == "new"


def test_compute_rate_divides_delta_by_elapsed_time():
    assert compute_rate(prev_count=100, curr_count=250, elapsed_s=5.0) == 30.0


def test_compute_rate_zero_elapsed_returns_zero_not_a_division_error():
    assert compute_rate(prev_count=100, curr_count=250, elapsed_s=0.0) == 0.0


def test_compute_rate_never_goes_negative_on_a_counter_reset():
    # e.g. a service restart resets request_count to 0 - the rate should
    # read 0, not a large negative number.
    assert compute_rate(prev_count=500, curr_count=10, elapsed_s=2.0) == 0.0


def test_campaign_label_none_reads_as_none():
    assert campaign_label(None, {}) == "(none)"


def test_campaign_label_unknown_id_falls_back_to_bare_id():
    assert campaign_label("house_ad", {}) == "house_ad"


def test_campaign_label_known_id_includes_category_and_advertiser():
    campaigns_by_id = {"c_0001": {"campaign_id": "c_0001", "category": "retail", "advertiser_name": "ExampleCo"}}
    assert campaign_label("c_0001", campaigns_by_id) == "c_0001 (retail, ExampleCo)"
