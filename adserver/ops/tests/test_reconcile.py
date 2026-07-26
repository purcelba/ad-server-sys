import shutil
import uuid

import polars as pl
import pytest
import redis as redis_lib

from adserver.adserver.pacing import decrement_capacity, get_remaining_capacity
from adserver.ops.reconcile import reconcile

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
    return f"reconciletest_{uuid.uuid4().hex[:8]}"


def _campaigns_df(campaign_id: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "campaign_id": campaign_id,
                "demand_type": "auction",
                "budget": 10.0,
                "impression_goal": None,
            }
        ]
    )


@requires_infra
def test_matching_decrements_and_wins_have_zero_discrepancy(redis_client, campaign_id):
    row = {"campaign_id": campaign_id, "demand_type": "auction", "budget": 10.0}
    for _ in range(3):
        get_remaining_capacity(redis_client, row)
        decrement_capacity(redis_client, row)

    decisions = [{"winner": campaign_id} for _ in range(3)] + [{"winner": None}]
    rows = reconcile(_campaigns_df(campaign_id), decisions, redis_client)

    assert len(rows) == 1
    assert rows[0]["expected_consumed"] == 3.0
    assert rows[0]["actual_served"] == 3
    assert rows[0]["discrepancy"] == 0.0


@requires_infra
def test_overshoot_race_produces_negative_discrepancy(redis_client, campaign_id):
    # Simulate the AC4 race directly: two concurrent decrements both
    # read the same stale value and both write "one less" - the counter
    # only reflects a single decrement even though both requests actually
    # won and got logged. Counter under-counts relative to the log: a
    # real, expected discrepancy, not a bug in this report.
    row = {"campaign_id": campaign_id, "demand_type": "auction", "budget": 10.0}
    current = get_remaining_capacity(redis_client, row)
    redis_client.set(f"pacing:budget_remaining:{campaign_id}", current - 1.0)
    redis_client.set(f"pacing:budget_remaining:{campaign_id}", current - 1.0)

    decisions = [{"winner": campaign_id}, {"winner": campaign_id}]
    rows = reconcile(_campaigns_df(campaign_id), decisions, redis_client)

    assert rows[0]["expected_consumed"] == 1.0
    assert rows[0]["actual_served"] == 2
    assert rows[0]["discrepancy"] == -1.0


@requires_infra
def test_campaign_never_touched_by_pacing_is_excluded(redis_client, campaign_id):
    decisions: list[dict] = []
    rows = reconcile(_campaigns_df(campaign_id), decisions, redis_client)
    assert rows == []
