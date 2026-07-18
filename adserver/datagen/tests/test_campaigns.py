import numpy as np

from adserver.datagen.campaigns import N_CAMPAIGNS, generate_campaigns
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
