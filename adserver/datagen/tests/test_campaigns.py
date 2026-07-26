import numpy as np
import polars as pl

from adserver.datagen.campaigns import AUDIENCE_TARGETING_BY_CATEGORY, N_CAMPAIGNS, generate_campaigns
from adserver.datagen.lifts import CATEGORIES


def test_campaign_count_and_schema():
    campaigns = generate_campaigns(np.random.default_rng(1))
    assert campaigns.height == N_CAMPAIGNS == 40
    assert campaigns["campaign_id"].n_unique() == N_CAMPAIGNS


def test_category_and_demand_type_split():
    campaigns = generate_campaigns(np.random.default_rng(1))
    counts = campaigns.group_by(["category", "demand_type"]).len()
    assert counts.height == len(CATEGORIES) * 2
    assert set(counts["len"].to_list()) == {4}


def test_nullability_rules_by_demand_type():
    campaigns = generate_campaigns(np.random.default_rng(1))
    auction = campaigns.filter(campaigns["demand_type"] == "auction")
    guaranteed = campaigns.filter(campaigns["demand_type"] == "guaranteed")

    assert auction["bid"].null_count() == 0
    assert auction["budget"].null_count() == 0
    assert auction["impression_goal"].null_count() == auction.height

    assert guaranteed["impression_goal"].null_count() == 0
    assert guaranteed["bid"].null_count() == guaranteed.height
    assert guaranteed["budget"].null_count() == guaranteed.height


def test_flight_dates_valid():
    campaigns = generate_campaigns(np.random.default_rng(1))
    assert (campaigns["flight_start"] < campaigns["flight_end"]).all()


def test_status_enum_not_degenerate():
    campaigns = generate_campaigns(np.random.default_rng(1))
    statuses = set(campaigns["status"].unique().to_list())
    assert statuses.issubset({"active", "paused", "ended"})
    assert len(statuses) > 1


def test_exactly_one_active_campaign_targets_each_named_audience():
    campaigns = generate_campaigns(np.random.default_rng(1))
    targeted = campaigns.filter(pl.col("targeted_audiences").list.len() > 0)

    assert targeted.height == len(AUDIENCE_TARGETING_BY_CATEGORY)
    assert set(targeted["status"].to_list()) == {"active"}
    got = {
        row["category"]: row["targeted_audiences"][0]
        for row in targeted.to_dicts()
    }
    assert got == AUDIENCE_TARGETING_BY_CATEGORY


def test_untargeted_campaigns_have_an_empty_list_not_null():
    campaigns = generate_campaigns(np.random.default_rng(1))
    untargeted = campaigns.filter(pl.col("targeted_audiences").list.len() == 0)
    assert untargeted.height == N_CAMPAIGNS - len(AUDIENCE_TARGETING_BY_CATEGORY)
    assert untargeted["targeted_audiences"].null_count() == 0


def test_generate_campaigns_is_idempotent_across_repeated_calls():
    """Guards against the module-level AUDIENCE_TARGETING_BY_CATEGORY dict
    being mutated by a prior call (a real bug caught during development:
    an early version used dict.pop() directly on the module constant,
    which left it empty and broke every call after the first)."""
    first = generate_campaigns(np.random.default_rng(1))
    second = generate_campaigns(np.random.default_rng(1))
    assert first.equals(second)
    assert AUDIENCE_TARGETING_BY_CATEGORY == {"travel": "frequent_airport_travelers", "transit": "weekday_commuters"}
