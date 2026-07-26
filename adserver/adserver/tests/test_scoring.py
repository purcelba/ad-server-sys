import datetime as dt

import pytest

from adserver.adserver.features import FeatureValue
from adserver.adserver.scoring import DESTINATION_MATCH_BOOST, score_candidates
from adserver.common.audiences import AudienceDef

NOW = dt.datetime(2026, 7, 20, 14, 30)
AUDIENCES: dict[str, AudienceDef] = {}


class _FixedScorer:
    """Stub Scorer — returns a fixed pCTR regardless of input, so tests
    can check eCPM math and wiring without a real trained model."""

    def score(self, feature_dict):
        return 0.1


def test_ecpm_is_bid_times_pctr_for_auction_candidates():
    user_features = {}
    candidates = [{"campaign_id": "c_0001", "category": "food", "demand_type": "auction", "bid": 2.0}]
    scored = score_candidates(_FixedScorer(), user_features, candidates, {}, AUDIENCES, NOW)
    assert scored[0].pctr == 0.1
    assert scored[0].ecpm == 0.2


def test_guaranteed_candidates_have_no_ecpm():
    user_features = {}
    candidates = [{"campaign_id": "c_0002", "category": "food", "demand_type": "guaranteed", "bid": None}]
    scored = score_candidates(_FixedScorer(), user_features, candidates, {}, AUDIENCES, NOW)
    assert scored[0].ecpm is None
    assert scored[0].pctr == 0.1


def test_missing_ad_features_still_scores_via_registry_defaults_upstream():
    """No entry in ad_features_by_campaign for this campaign_id - assemble
    still produces a (default-substituted) feature dict rather than
    raising; scoring shouldn't need to special-case it."""
    candidates = [{"campaign_id": "c_missing", "category": "food", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), {}, candidates, {}, AUDIENCES, NOW)
    assert scored[0].campaign_id == "c_missing"
    assert scored[0].pctr == 0.1


def test_hour_of_day_context_feature_comes_from_the_injected_now():
    candidates = [{"campaign_id": "c_0001", "category": "food", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), {}, candidates, {}, AUDIENCES, NOW)
    assert scored[0].features["hour_of_day"] == 14


def test_feature_value_objects_are_unwrapped_via_assemble():
    ad_features = {"c_0001": {"ad_ctr_7d": FeatureValue(value=0.05, freshness_status="fresh")}}
    candidates = [{"campaign_id": "c_0001", "category": "food", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), {}, candidates, ad_features, AUDIENCES, NOW)
    assert scored[0].features["ad_ctr_7d"] == 0.05


def _destination(value: str) -> dict:
    return {"user_current_destination_category": FeatureValue(value=value, freshness_status="fresh")}


def test_matching_real_time_destination_boosts_the_score():
    candidates = [{"campaign_id": "c_travel", "category": "travel", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), _destination("travel"), candidates, {}, AUDIENCES, NOW)
    assert scored[0].pctr == pytest.approx(0.1 * DESTINATION_MATCH_BOOST)


def test_non_matching_destination_does_not_boost():
    candidates = [{"campaign_id": "c_travel", "category": "travel", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), _destination("food"), candidates, {}, AUDIENCES, NOW)
    assert scored[0].pctr == 0.1


def test_empty_destination_does_not_boost():
    candidates = [{"campaign_id": "c_travel", "category": "travel", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_FixedScorer(), _destination(""), candidates, {}, AUDIENCES, NOW)
    assert scored[0].pctr == 0.1


def test_boost_is_clamped_to_a_valid_probability():
    class _HighScorer:
        def score(self, feature_dict):
            return 0.9

    candidates = [{"campaign_id": "c_travel", "category": "travel", "demand_type": "auction", "bid": 1.0}]
    scored = score_candidates(_HighScorer(), _destination("travel"), candidates, {}, AUDIENCES, NOW)
    assert scored[0].pctr == 1.0
