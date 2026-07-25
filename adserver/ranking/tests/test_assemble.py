from adserver.feature_service.resolver import FeatureResult
from adserver.ranking.assemble import from_offline_row, from_online_result


def test_from_offline_row_merges_user_and_ad_features_and_adds_the_cross():
    user_row = {
        "user_id": "u_0001",
        "asof": "2026-07-01",
        "entity": "user",
        "user_ctr_30d": 0.05,
        "user_ctr_by_category_30d": {"travel": 0.2},
    }
    ad_row = {
        "campaign_id": "c_0001",
        "asof": "2026-07-01",
        "entity": "ad",
        "ad_ctr_7d": 0.03,
    }

    result = from_offline_row(user_row, ad_row, ad_category="travel")

    assert result["user_ctr_30d"] == 0.05
    assert result["ad_ctr_7d"] == 0.03
    assert result["x_user_ctr_in_ad_category"] == 0.2
    # metadata columns must not leak into the assembled feature dict
    assert "user_id" not in result
    assert "asof" not in result
    assert "entity" not in result


def test_from_online_result_unwraps_feature_result_values_and_adds_the_cross():
    user_result = {
        "user_ctr_30d": FeatureResult(value=0.05, computed_at="t", freshness_status="fresh", default_substituted=False),
        "user_ctr_by_category_30d": FeatureResult(
            value={"travel": 0.2}, computed_at="t", freshness_status="fresh", default_substituted=False
        ),
    }
    ad_result = {
        "ad_ctr_7d": FeatureResult(value=0.03, computed_at="t", freshness_status="fresh", default_substituted=False),
    }

    result = from_online_result(user_result, ad_result, ad_category="travel")

    assert result["user_ctr_30d"] == 0.05
    assert result["ad_ctr_7d"] == 0.03
    assert result["x_user_ctr_in_ad_category"] == 0.2


def test_both_adapters_produce_the_same_assembled_dict_for_equivalent_inputs():
    """A cheap sanity check that the two adapters really do normalize to
    the same shape — the real end-to-end version of this lives in
    test_ac6_cross_parity.py, against live infra."""
    offline_row_user = {"user_id": "u_1", "asof": "d", "entity": "user", "user_ctr_by_category_30d": {"food": 0.1}}
    offline_row_ad = {"campaign_id": "c_1", "asof": "d", "entity": "ad", "ad_ctr_7d": 0.04}

    online_user = {
        "user_ctr_by_category_30d": FeatureResult(
            value={"food": 0.1}, computed_at="t", freshness_status="fresh", default_substituted=False
        )
    }
    online_ad = {
        "ad_ctr_7d": FeatureResult(value=0.04, computed_at="t", freshness_status="fresh", default_substituted=False)
    }

    assert from_offline_row(offline_row_user, offline_row_ad, "food") == from_online_result(
        online_user, online_ad, "food"
    )
