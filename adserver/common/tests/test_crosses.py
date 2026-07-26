from adserver.common.audiences import AudienceDef, Rule
from adserver.common.crosses import (
    CROSS_FUNCTIONS,
    x_user_ctr_in_ad_category,
    x_user_in_audience_matching_ad_category,
)

TRAVELER_AUDIENCE = AudienceDef(
    name="frequent_airport_travelers",
    definition_version=1,
    rules=(
        Rule(feature="segment", op="eq", value="traveler"),
        Rule(feature="user_ctr_by_category_30d.travel", op="gte", value=0.05),
    ),
)
AUDIENCES = {"frequent_airport_travelers": TRAVELER_AUDIENCE}


def test_pulls_the_matching_category_out_of_the_map():
    user_features = {"user_ctr_by_category_30d": {"travel": 0.12, "food": 0.03}}
    assert x_user_ctr_in_ad_category(user_features, "travel") == 0.12
    assert x_user_ctr_in_ad_category(user_features, "food") == 0.03


def test_defaults_to_zero_for_a_category_with_no_entry():
    user_features = {"user_ctr_by_category_30d": {"travel": 0.12}}
    assert x_user_ctr_in_ad_category(user_features, "retail") == 0.0


def test_defaults_to_zero_when_the_map_is_missing_or_empty():
    assert x_user_ctr_in_ad_category({}, "travel") == 0.0
    assert x_user_ctr_in_ad_category({"user_ctr_by_category_30d": {}}, "travel") == 0.0
    assert x_user_ctr_in_ad_category({"user_ctr_by_category_30d": None}, "travel") == 0.0


def test_registered_under_its_own_name():
    assert CROSS_FUNCTIONS["x_user_ctr_in_ad_category"] is x_user_ctr_in_ad_category
    assert CROSS_FUNCTIONS["x_user_in_audience_matching_ad_category"] is x_user_in_audience_matching_ad_category


def test_audience_affinity_true_when_a_membership_rule_references_the_ad_category():
    user_features = {"audience_memberships": ["frequent_airport_travelers"]}
    assert x_user_in_audience_matching_ad_category(user_features, "travel", AUDIENCES) is True


def test_audience_affinity_false_for_a_non_matching_category():
    user_features = {"audience_memberships": ["frequent_airport_travelers"]}
    assert x_user_in_audience_matching_ad_category(user_features, "food", AUDIENCES) is False


def test_audience_affinity_false_when_user_has_no_memberships():
    assert x_user_in_audience_matching_ad_category({}, "travel", AUDIENCES) is False
    assert x_user_in_audience_matching_ad_category(
        {"audience_memberships": []}, "travel", AUDIENCES
    ) is False


def test_audience_affinity_ignores_an_unknown_audience_name_gracefully():
    """A stale audience_membership value (e.g. a definition later removed
    from audiences.yaml) shouldn't raise — just doesn't match."""
    user_features = {"audience_memberships": ["some_retired_audience"]}
    assert x_user_in_audience_matching_ad_category(user_features, "travel", AUDIENCES) is False
