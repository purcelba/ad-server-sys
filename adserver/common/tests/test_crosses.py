from adserver.common.crosses import CROSS_FUNCTIONS, x_user_ctr_in_ad_category


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
