import datetime as dt

import polars as pl
import pytest

from adserver.adserver.retrieval import retrieve_candidates
from adserver.common.audiences import AudienceDef, Rule

TODAY = dt.date(2026, 7, 20)

AUDIENCES = {
    "frequent_airport_travelers": AudienceDef(
        name="frequent_airport_travelers",
        definition_version=1,
        rules=(Rule(feature="segment", op="eq", value="traveler"),),
    ),
}


def _campaigns_df(rows: list[dict]) -> pl.DataFrame:
    defaults = {
        "campaign_id": "c_0000",
        "advertiser_name": "Test Co",
        "category": "food",
        "demand_type": "auction",
        "bid": 1.0,
        "budget": 100.0,
        "impression_goal": None,
        "flight_start": TODAY - dt.timedelta(days=5),
        "flight_end": TODAY + dt.timedelta(days=5),
        "status": "active",
        "targeted_audiences": [],
    }
    return pl.DataFrame([{**defaults, **row} for row in rows])


def _unlimited(_row: dict) -> float:
    return 1000.0


def test_active_untargeted_campaign_is_eligible():
    campaigns = _campaigns_df([{"campaign_id": "c_0001"}])
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, _unlimited)
    assert [c["campaign_id"] for c in result.eligible] == ["c_0001"]
    assert result.excluded_by_audience == []


def test_paused_campaign_is_excluded_not_flagged_as_audience_exclusion():
    campaigns = _campaigns_df([{"campaign_id": "c_0001", "status": "paused"}])
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, _unlimited)
    assert result.eligible == []
    assert result.excluded_by_audience == []  # excluded on status, not audience


def test_campaign_outside_its_flight_is_excluded():
    campaigns = _campaigns_df(
        [{"campaign_id": "c_0001", "flight_start": TODAY + dt.timedelta(days=1), "flight_end": TODAY + dt.timedelta(days=10)}]
    )
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, _unlimited)
    assert result.eligible == []


def test_campaign_with_zero_remaining_capacity_is_excluded():
    campaigns = _campaigns_df([{"campaign_id": "c_0001"}])
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, lambda row: 0.0)
    assert result.eligible == []


def test_targeted_campaign_ineligible_for_non_member():
    campaigns = _campaigns_df([{"campaign_id": "c_0001", "targeted_audiences": ["frequent_airport_travelers"]}])
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, _unlimited)
    assert result.eligible == []
    assert len(result.excluded_by_audience) == 1
    exclusion = result.excluded_by_audience[0]
    assert exclusion.campaign_id == "c_0001"
    assert exclusion.audience == "frequent_airport_travelers"
    assert exclusion.definition_version == 1


def test_targeted_campaign_eligible_for_member():
    campaigns = _campaigns_df([{"campaign_id": "c_0001", "targeted_audiences": ["frequent_airport_travelers"]}])
    result = retrieve_candidates(campaigns, TODAY, ["frequent_airport_travelers"], AUDIENCES, _unlimited)
    assert [c["campaign_id"] for c in result.eligible] == ["c_0001"]
    assert result.excluded_by_audience == []


def test_member_still_receives_untargeted_campaigns_from_other_categories():
    """Proves audiences gate eligibility, not relevance: a member of the
    audience still sees plain, non-targeted campaigns normally."""
    campaigns = _campaigns_df(
        [
            {"campaign_id": "c_travel", "category": "travel", "targeted_audiences": ["frequent_airport_travelers"]},
            {"campaign_id": "c_food", "category": "food", "targeted_audiences": []},
        ]
    )
    result = retrieve_candidates(campaigns, TODAY, ["frequent_airport_travelers"], AUDIENCES, _unlimited)
    assert {c["campaign_id"] for c in result.eligible} == {"c_travel", "c_food"}


def test_non_member_still_receives_untargeted_campaigns():
    campaigns = _campaigns_df(
        [
            {"campaign_id": "c_travel", "category": "travel", "targeted_audiences": ["frequent_airport_travelers"]},
            {"campaign_id": "c_food", "category": "food", "targeted_audiences": []},
        ]
    )
    result = retrieve_candidates(campaigns, TODAY, [], AUDIENCES, _unlimited)
    assert [c["campaign_id"] for c in result.eligible] == ["c_food"]
    assert len(result.excluded_by_audience) == 1
