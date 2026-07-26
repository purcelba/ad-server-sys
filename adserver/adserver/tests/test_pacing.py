import datetime as dt
import shutil
import uuid

import pytest
import redis as redis_lib

from adserver.adserver.pacing import (
    ArbitrationResult,
    arbitrate,
    decrement_capacity,
    elapsed_flight_fraction,
    get_remaining_capacity,
    is_behind_schedule,
    record_delivery,
)
from adserver.adserver.scoring import ScoredCandidate

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        redis_lib.Redis(host="localhost", port=6379).ping()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(not _infra_reachable(), reason="redis not reachable — run `make up`")


@pytest.fixture
def redis_client():
    return redis_lib.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture
def campaign_id():
    return f"pacingtest_{uuid.uuid4().hex[:8]}"


def _auction_row(campaign_id: str, budget: float = 10.0) -> dict:
    return {"campaign_id": campaign_id, "demand_type": "auction", "budget": budget}


def _guaranteed_row(campaign_id: str, impression_goal: int = 1000, flight_start=None, flight_end=None) -> dict:
    return {
        "campaign_id": campaign_id,
        "demand_type": "guaranteed",
        "impression_goal": impression_goal,
        "flight_start": flight_start or dt.date(2026, 7, 1),
        "flight_end": flight_end or dt.date(2026, 7, 11),
    }


@requires_infra
def test_get_remaining_capacity_initializes_from_catalog_value(redis_client, campaign_id):
    row = _auction_row(campaign_id, budget=42.0)
    assert get_remaining_capacity(redis_client, row) == 42.0


@requires_infra
def test_decrement_capacity_reduces_by_amount(redis_client, campaign_id):
    row = _auction_row(campaign_id, budget=10.0)
    get_remaining_capacity(redis_client, row)
    decrement_capacity(redis_client, row, amount=3.0)
    assert get_remaining_capacity(redis_client, row) == 7.0


@requires_infra
def test_guaranteed_capacity_keyed_separately_from_auction_budget(redis_client, campaign_id):
    guaranteed = _guaranteed_row(campaign_id, impression_goal=500)
    assert get_remaining_capacity(redis_client, guaranteed) == 500.0


@requires_infra
def test_record_delivery_increments_and_returns_running_count(redis_client, campaign_id):
    row = _guaranteed_row(campaign_id)
    assert record_delivery(redis_client, row) == 1
    assert record_delivery(redis_client, row) == 2
    assert record_delivery(redis_client, row) == 3


def test_elapsed_flight_fraction_at_start_middle_end():
    start, end = dt.date(2026, 7, 1), dt.date(2026, 7, 11)  # 10-day flight
    assert elapsed_flight_fraction(start, start, end) == 0.0
    assert elapsed_flight_fraction(dt.date(2026, 7, 6), start, end) == 0.5
    assert elapsed_flight_fraction(end, start, end) == 1.0
    assert elapsed_flight_fraction(dt.date(2026, 8, 1), start, end) == 1.0  # clamped


def test_is_behind_schedule_true_when_delivery_lags():
    row = _guaranteed_row("c_1", impression_goal=1000, flight_start=dt.date(2026, 7, 1), flight_end=dt.date(2026, 7, 11))
    # halfway through the flight, expected ~500 delivered
    assert is_behind_schedule(row, dt.date(2026, 7, 6), delivered=100) is True
    assert is_behind_schedule(row, dt.date(2026, 7, 6), delivered=600) is False


def test_arbitrate_behind_schedule_guaranteed_wins_outright():
    guaranteed = [_guaranteed_row("c_guaranteed", impression_goal=1000, flight_start=dt.date(2026, 7, 1), flight_end=dt.date(2026, 7, 11))]
    auction = [ScoredCandidate(campaign_id="c_auction", demand_type="auction", pctr=0.5, ecpm=100.0, features={})]
    result = arbitrate(guaranteed, auction, external_bid=None, now=dt.date(2026, 7, 6), delivered_by_campaign={"c_guaranteed": 0})
    assert result == ArbitrationResult(winner_campaign_id="c_guaranteed", rung="guaranteed", price=None)


def test_arbitrate_ahead_of_schedule_guaranteed_lets_auction_compete():
    guaranteed = [_guaranteed_row("c_guaranteed", impression_goal=1000, flight_start=dt.date(2026, 7, 1), flight_end=dt.date(2026, 7, 11))]
    auction = [ScoredCandidate(campaign_id="c_auction", demand_type="auction", pctr=0.5, ecpm=100.0, features={})]
    result = arbitrate(guaranteed, auction, external_bid=None, now=dt.date(2026, 7, 6), delivered_by_campaign={"c_guaranteed": 900})
    assert result == ArbitrationResult(winner_campaign_id="c_auction", rung="auction", price=100.0)


def test_arbitrate_highest_ecpm_wins_among_internal_auction_candidates():
    auction = [
        ScoredCandidate(campaign_id="c_low", demand_type="auction", pctr=0.1, ecpm=10.0, features={}),
        ScoredCandidate(campaign_id="c_high", demand_type="auction", pctr=0.5, ecpm=50.0, features={}),
    ]
    result = arbitrate([], auction, external_bid=None, now=dt.date(2026, 7, 6), delivered_by_campaign={})
    assert result.winner_campaign_id == "c_high"
    assert result.rung == "auction"


def test_arbitrate_external_bid_wins_when_higher_than_internal():
    auction = [ScoredCandidate(campaign_id="c_internal", demand_type="auction", pctr=0.1, ecpm=10.0, features={})]
    result = arbitrate([], auction, external_bid=25.0, now=dt.date(2026, 7, 6), delivered_by_campaign={})
    assert result == ArbitrationResult(winner_campaign_id=None, rung="external", price=25.0)


def test_arbitrate_internal_wins_when_higher_than_external():
    auction = [ScoredCandidate(campaign_id="c_internal", demand_type="auction", pctr=0.1, ecpm=30.0, features={})]
    result = arbitrate([], auction, external_bid=5.0, now=dt.date(2026, 7, 6), delivered_by_campaign={})
    assert result.winner_campaign_id == "c_internal"
    assert result.rung == "auction"


def test_arbitrate_no_candidates_at_all_returns_none_rung():
    result = arbitrate([], [], external_bid=None, now=dt.date(2026, 7, 6), delivered_by_campaign={})
    assert result == ArbitrationResult(winner_campaign_id=None, rung="none", price=None)
