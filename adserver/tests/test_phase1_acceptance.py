"""Phase 1 acceptance criteria (phases.md).

AC1 and AC2, run per explicit instruction (one at a time). AC1 requires
`make up` (dynamodb-local) — self-skips without it, matching
test_phase0_acceptance.py's pattern. AC2 needs no infra.
"""

import datetime as dt
import shutil

import polars as pl
import pytest

from adserver.batch_features.jobs.user_ctr import UserCtrJob
from adserver.batch_features.materialize import TABLE_NAME, get_resource
from adserver.batch_features.offline_store import query_as_of
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH, run
from adserver.common.registry import load_registry
from adserver.datagen.users import HISTORY_END

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _dynamo_reachable() -> bool:
    try:
        get_resource().meta.client.list_tables(Limit=1)
        return True
    except Exception:
        return False


requires_dynamo = pytest.mark.skipif(
    not _dynamo_reachable(), reason="dynamodb-local not reachable — run `make up`"
)


def _scan_all_items() -> list[dict]:
    resource = get_resource()
    table = resource.Table(TABLE_NAME)
    items: list[dict] = []
    resp = table.scan()
    items.extend(resp["Items"])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp["Items"])
    return items


@requires_dynamo
def test_ac1_make_features_computes_and_materializes_all_registry_features_idempotently():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    expected_feature_names = set(registry.keys())
    assert len(expected_feature_names) == 9  # sanity: registry hasn't silently shrunk

    # --- first run ---
    combined_1 = run(as_of=HISTORY_END, materialize_to_dynamo=True)
    assert combined_1["user"].height == 50
    assert combined_1["ad"].height == 40

    items_1 = _scan_all_items()
    assert len(items_1) > 0, "materialization wrote nothing"

    materialized_feature_names = {item["feature_name"] for item in items_1}
    assert materialized_feature_names == expected_feature_names, (
        f"missing from DynamoDB: {expected_feature_names - materialized_feature_names}, "
        f"unexpected: {materialized_feature_names - expected_feature_names}"
    )

    # every registry feature must be materialized for at least one entity
    counts_by_feature = {}
    for item in items_1:
        counts_by_feature[item["feature_name"]] = counts_by_feature.get(item["feature_name"], 0) + 1
    for name in expected_feature_names:
        assert counts_by_feature.get(name, 0) > 0, f"{name} was declared but never materialized"

    values_1 = {(item["entity_key"], item["feature_name"]): item["value"] for item in items_1}

    # --- second run: same as_of, no new data generated in between ---
    combined_2 = run(as_of=HISTORY_END, materialize_to_dynamo=True)
    assert combined_2["user"].height == combined_1["user"].height
    assert combined_2["ad"].height == combined_1["ad"].height

    items_2 = _scan_all_items()
    values_2 = {(item["entity_key"], item["feature_name"]): item["value"] for item in items_2}

    # idempotent: identical (entity, feature) keys and identical values.
    # computed_at is intentionally excluded from this comparison - it's
    # expected to advance on every run; the feature *values* must not.
    assert set(values_1.keys()) == set(values_2.keys())
    mismatches = {k: (values_1[k], values_2[k]) for k in values_1 if values_1[k] != values_2[k]}
    assert not mismatches, f"non-idempotent values on re-run: {mismatches}"


# --- AC2: point-in-time test ---
# "querying features 'as of' day 15 returns values computed only from days 1-15."
#
# A controlled synthetic dataset with a deliberately planted "future" event
# (dated after the as_of boundary) - this doesn't just check that numbers
# happen to match, it proves the boundary is actually enforced: if the job
# leaked the future event in, this test would catch it by construction, not
# by coincidence.

DAY_1 = dt.date(2026, 1, 1)
AS_OF_DAY_15 = dt.date(2026, 1, 15)
FUTURE_DAY_20 = dt.date(2026, 1, 20)  # after AS_OF_DAY_15 - must never be visible


def _write_synthetic_dataset(data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"user_id": ["u_test"], "segment": ["general"], "home_metro": ["seattle"], "created_at": [DAY_1]}
    ).write_parquet(data_dir / "users.parquet")

    events = pl.DataFrame(
        [
            # within the as_of=day15 window - must be included
            {
                "event_id": "e_1", "event_type": "impression", "user_id": "u_test",
                "campaign_id": "c_test", "category": "food", "segment": "general",
                "ts": dt.datetime.combine(DAY_1 + dt.timedelta(days=9), dt.time(12, 0)),
                "event_date": DAY_1 + dt.timedelta(days=9), "hour_of_day": 12, "click_id": None,
            },
            {
                "event_id": "e_2", "event_type": "click", "user_id": "u_test",
                "campaign_id": "c_test", "category": "food", "segment": "general",
                "ts": dt.datetime.combine(DAY_1 + dt.timedelta(days=9), dt.time(12, 1)),
                "event_date": DAY_1 + dt.timedelta(days=9), "hour_of_day": 12, "click_id": "e_1",
            },
            # planted FUTURE event, after as_of=day15 - a no-click impression;
            # if leaked in, it would dilute CTR from 1.0 down to 0.5
            {
                "event_id": "e_3", "event_type": "impression", "user_id": "u_test",
                "campaign_id": "c_test", "category": "food", "segment": "general",
                "ts": dt.datetime.combine(FUTURE_DAY_20, dt.time(9, 0)),
                "event_date": FUTURE_DAY_20, "hour_of_day": 9, "click_id": None,
            },
        ],
        schema={
            "event_id": pl.Utf8, "event_type": pl.Utf8, "user_id": pl.Utf8, "campaign_id": pl.Utf8,
            "category": pl.Utf8, "segment": pl.Utf8, "ts": pl.Datetime(time_unit="us"),
            "event_date": pl.Date, "hour_of_day": pl.Int64, "click_id": pl.Utf8,
        },
    )
    events.write_parquet(data_dir / "events.parquet")

    # rides within both as_of windows this test exercises (day15's trailing
    # 7d = day9-15, day20's trailing 7d = day14-20), so UserRidesPerWeekJob
    # has full 1/1 coverage at both as_of points and the quality gate
    # doesn't (correctly) block on an unrelated job while this test is only
    # exercising CTR point-in-time behavior
    pl.DataFrame(
        {
            "ride_id": ["r_1", "r_2"], "user_id": ["u_test", "u_test"],
            "ts": [
                dt.datetime.combine(DAY_1 + dt.timedelta(days=9), dt.time(8, 0)),
                dt.datetime.combine(DAY_1 + dt.timedelta(days=18), dt.time(8, 0)),
            ],
            "ride_date": [DAY_1 + dt.timedelta(days=9), DAY_1 + dt.timedelta(days=18)],
            "ride_type": ["standard", "standard"],
        },
        schema={
            "ride_id": pl.Utf8, "user_id": pl.Utf8, "ts": pl.Datetime(time_unit="us"),
            "ride_date": pl.Date, "ride_type": pl.Utf8,
        },
    ).write_parquet(data_dir / "rides.parquet")

    pl.DataFrame(
        {"campaign_id": [], "advertiser_name": [], "category": [], "demand_type": [], "bid": [],
         "budget": [], "impression_goal": [], "flight_start": [], "flight_end": [], "status": []},
        schema={
            "campaign_id": pl.Utf8, "advertiser_name": pl.Utf8, "category": pl.Utf8,
            "demand_type": pl.Utf8, "bid": pl.Float64, "budget": pl.Float64,
            "impression_goal": pl.Int64, "flight_start": pl.Date, "flight_end": pl.Date,
            "status": pl.Utf8,
        },
    ).write_parquet(data_dir / "campaigns.parquet")


def test_ac2_point_in_time_query_never_leaks_future_data(tmp_path):
    data_dir = tmp_path / "data"
    _write_synthetic_dataset(data_dir)

    # --- direct job compute(), the real production code path ---
    df = UserCtrJob().compute(AS_OF_DAY_15, data_dir=data_dir)
    ctr = df.filter(df["user_id"] == "u_test")["user_ctr_30d"].item()
    assert ctr == 1.0, (
        f"expected ctr=1.0 (1 impression, 1 click, both within days 1-15) but got {ctr} - "
        f"the future (day 20) impression appears to have leaked into an as_of=day15 query"
    )

    # --- through the offline store's point-in-time DuckDB query layer ---
    output_dir = tmp_path / "features"
    run(as_of=AS_OF_DAY_15, data_dir=data_dir, output_dir=output_dir)

    result = query_as_of("user", AS_OF_DAY_15, output_dir)
    stored_ctr = result.filter(result["user_id"] == "u_test")["user_ctr_30d"].item()
    assert stored_ctr == 1.0

    # --- proof the test has power to catch leakage: a later as_of DOES see it ---
    later_df = UserCtrJob().compute(FUTURE_DAY_20, data_dir=data_dir)
    later_ctr = later_df.filter(later_df["user_id"] == "u_test")["user_ctr_30d"].item()
    assert later_ctr == 0.5, (
        "sanity check failed: as_of=day20 should see both impressions (1 click / 2 "
        f"impressions = 0.5), got {later_ctr} - if this fails, the planted future event "
        "isn't actually distinguishable, which would make the day-15 assertion above "
        "meaningless rather than a real point-in-time proof"
    )

    # --- and that later partition existing doesn't retroactively leak into
    #     a day-15 query against the offline store ---
    run(as_of=FUTURE_DAY_20, data_dir=data_dir, output_dir=output_dir)
    still_day15 = query_as_of("user", AS_OF_DAY_15, output_dir)
    still_ctr = still_day15.filter(still_day15["user_id"] == "u_test")["user_ctr_30d"].item()
    assert still_ctr == 1.0, "a later partition being written must not change what an earlier as_of query returns"
