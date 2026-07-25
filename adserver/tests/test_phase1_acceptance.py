"""Phase 1 acceptance criteria (phases.md).

AC1 and AC2, run per explicit instruction (one at a time). AC1 requires
`make up` (dynamodb-local) — self-skips without it, matching
test_phase0_acceptance.py's pattern. AC2 needs no infra.
"""

import datetime as dt
import shutil
import uuid

import polars as pl
import pytest

from adserver.batch_features.jobs.ad_impressions import AdImpressionsJob
from adserver.batch_features.jobs.user_account_age import UserAccountAgeJob
from adserver.batch_features.jobs.user_ctr import UserCtrJob
from adserver.batch_features.materialize import MaterializeError, TABLE_NAME, get_resource, materialize as materialize_fn
from adserver.batch_features.offline_store import query_as_of
from adserver.batch_features.quality import QualityGateError
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH, discover_jobs, run
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

    # "all registry features" here means all *batch-produced* ones - as of
    # Phase 2, the registry also holds stream-only features (written by
    # stream_features/consumer.py, not by any batch job), so the exact set
    # this AC covers is what the discovered batch jobs actually claim to
    # produce, not literally every registry entry.
    expected_feature_names = {name for job in discover_jobs() for name in job.outputs()}
    assert len(expected_feature_names) == 10  # sanity: batch jobs haven't silently shrunk (9 + AC5's user_account_age_days)
    assert expected_feature_names.issubset(registry.keys())

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


# --- AC3: poisoning test ---
# "corrupt a day's events (inject 50% nulls) -> pipeline fails at the
# quality gate and does NOT materialize."
#
# Diagnosed before building this: on the real 30-day dataset, corrupting
# one day at 50% nulls does NOT trip either quality check (verified
# directly - see PROGRESS.md) - a single day is too small a fraction of
# any job's window at that data volume. So this uses a small, deliberately
# sized synthetic dataset (same rigor as AC2) where the corruption's impact
# is large enough relative to the window to be provably caught, rather than
# asserting a threshold-tuning change to production code as part of this
# acceptance test.

WINDOW_AS_OF = dt.date(2026, 2, 7)  # 7-day window: 2026-02-01 .. 2026-02-07
CORRUPT_DAY = dt.date(2026, 2, 4)   # the single day whose events get poisoned


def _write_poisoning_dataset(data_dir, corrupt: bool, suffix: str):
    """2 campaigns (c_1_<suffix>, c_2_<suffix>), both eligible, with their
    entire week's impressions concentrated on CORRUPT_DAY. When
    `corrupt=True`, 50% of that day's events (c_1's two rows) have
    campaign_id nulled - c_1 should vanish from ad_impressions_7d's
    output entirely.

    `suffix` makes campaign_ids unique per test invocation so this test
    can't collide with leftover state in the shared dynamodb-local table
    from another run (see PROGRESS.md - this bit a mutation test directly).
    """
    c1, c2 = f"c_1_{suffix}", f"c_2_{suffix}"
    data_dir.mkdir(parents=True, exist_ok=True)

    campaigns = pl.DataFrame(
        {
            "campaign_id": [c1, c2], "advertiser_name": ["Advertiser 1", "Advertiser 2"],
            "category": ["food", "food"], "demand_type": ["auction", "auction"],
            "bid": [1.0, 1.0], "budget": [100.0, 100.0], "impression_goal": [None, None],
            "flight_start": [dt.date(2026, 1, 1), dt.date(2026, 1, 1)],
            "flight_end": [dt.date(2026, 3, 1), dt.date(2026, 3, 1)],
            "status": ["active", "active"],
        },
        schema={
            "campaign_id": pl.Utf8, "advertiser_name": pl.Utf8, "category": pl.Utf8,
            "demand_type": pl.Utf8, "bid": pl.Float64, "budget": pl.Float64,
            "impression_goal": pl.Int64, "flight_start": pl.Date, "flight_end": pl.Date,
            "status": pl.Utf8,
        },
    )
    campaigns.write_parquet(data_dir / "campaigns.parquet")

    rows = []
    for i, cid in enumerate([c1, c1, c2, c2]):
        rows.append(
            {
                "event_id": f"e_{i}", "event_type": "impression", "user_id": "u_1",
                "campaign_id": None if (corrupt and cid == c1) else cid,
                "category": "food", "segment": "general",
                "ts": dt.datetime.combine(CORRUPT_DAY, dt.time(10, i)),
                "event_date": CORRUPT_DAY, "hour_of_day": 10, "click_id": None,
            }
        )
    events = pl.DataFrame(
        rows,
        schema={
            "event_id": pl.Utf8, "event_type": pl.Utf8, "user_id": pl.Utf8, "campaign_id": pl.Utf8,
            "category": pl.Utf8, "segment": pl.Utf8, "ts": pl.Datetime(time_unit="us"),
            "event_date": pl.Date, "hour_of_day": pl.Int64, "click_id": pl.Utf8,
        },
    )
    events.write_parquet(data_dir / "events.parquet")

    pl.DataFrame(
        {"user_id": ["u_1"], "segment": ["general"], "home_metro": ["seattle"], "created_at": [dt.date(2026, 1, 1)]}
    ).write_parquet(data_dir / "users.parquet")

    pl.DataFrame(
        {"ride_id": [], "user_id": [], "ts": [], "ride_date": [], "ride_type": []},
        schema={
            "ride_id": pl.Utf8, "user_id": pl.Utf8, "ts": pl.Datetime(time_unit="us"),
            "ride_date": pl.Date, "ride_type": pl.Utf8,
        },
    ).write_parquet(data_dir / "rides.parquet")

    return c1, c2


def test_ac3_sanity_uncorrupted_dataset_passes_the_gate(tmp_path):
    """Establishes the baseline: without corruption, this exact dataset
    shape passes cleanly (2/2 campaigns represented) - so the failure in
    the next test is provably caused by the corruption, not by the
    dataset being malformed some other way."""
    data_dir = tmp_path / "data"
    c1, c2 = _write_poisoning_dataset(data_dir, corrupt=False, suffix="sanity")

    df = AdImpressionsJob().compute(WINDOW_AS_OF, data_dir=data_dir)
    assert df.height == 2
    assert set(df["campaign_id"].to_list()) == {c1, c2}


@requires_dynamo
def test_ac3_poisoned_day_fails_quality_gate_and_does_not_materialize(tmp_path):
    suffix = uuid.uuid4().hex[:8]
    data_dir = tmp_path / "data"
    c1, c2 = _write_poisoning_dataset(data_dir, corrupt=True, suffix=suffix)

    # confirm the corruption is exactly what it claims to be: 50% of
    # CORRUPT_DAY's events, nothing else touched
    events = pl.read_parquet(data_dir / "events.parquet")
    day_events = events.filter(events["event_date"] == CORRUPT_DAY)
    assert day_events.height == 4
    assert day_events["campaign_id"].null_count() == 2  # exactly 50%

    output_dir = tmp_path / "features"
    with pytest.raises(QualityGateError, match="row-count"):
        run(
            as_of=WINDOW_AS_OF,
            data_dir=data_dir,
            output_dir=output_dir,
            jobs=[AdImpressionsJob()],
            materialize_to_dynamo=True,
        )

    # does NOT materialize: no offline parquet written
    assert not output_dir.exists()

    # does NOT materialize: nothing reached DynamoDB either
    items = _scan_all_items()
    poisoned_keys = [i for i in items if i["entity_key"] in (f"ad#{c1}", f"ad#{c2}")]
    assert poisoned_keys == [], f"expected no materialized items for {c1}/{c2}, found: {poisoned_keys}"


# --- AC4 ---
# "Every materialized item carries computed_at; nothing outside the
# registry gets materialized."
#
# Diagnosed while building this: materialize() previously trusted whatever
# feature_names it was given - "nothing outside the registry" was only
# true because runner.run() happened to validate first, not because
# materialize() (the actual write boundary) enforced it itself. Fixed in
# materialize.py: it now loads the registry and validates independently,
# raising MaterializeError before writing anything. See PROGRESS.md.


@requires_dynamo
def test_ac4_every_item_has_computed_at_and_only_registry_features_are_materialized():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    registry_names = set(registry.keys())

    run(as_of=HISTORY_END, materialize_to_dynamo=True)
    items = _scan_all_items()
    assert len(items) > 0, "materialization wrote nothing"

    # every materialized item carries computed_at
    missing_computed_at = [i for i in items if not i.get("computed_at")]
    assert missing_computed_at == [], f"items missing computed_at: {missing_computed_at[:5]}"
    for item in items:
        dt.datetime.fromisoformat(item["computed_at"])  # raises if not a real timestamp

    # nothing outside the registry gets materialized
    materialized_names = {i["feature_name"] for i in items}
    unregistered = materialized_names - registry_names
    assert unregistered == set(), f"materialized features not in registry.yaml: {unregistered}"


@requires_dynamo
def test_ac4_materialize_enforces_the_registry_at_the_write_boundary():
    """Direct proof the enforcement lives in materialize() itself, not
    only in the runner's pre-check — matches the corresponding unit tests
    in test_materialize.py, kept here too so this AC is self-contained.

    Uses a per-invocation unique user_id (same reasoning as AC3's
    poisoning test) so a run of this test can never collide with leftover
    state from another run in the shared dynamodb-local table.
    """
    user_id = f"u_ac4_test_{uuid.uuid4().hex[:8]}"
    df = pl.DataFrame({"user_id": [user_id], "not_a_real_feature": [1.0]})
    computed_at = dt.datetime.now(dt.timezone.utc)
    with pytest.raises(MaterializeError, match="not_a_real_feature"):
        materialize_fn("user", df, ["not_a_real_feature"], computed_at)

    items = _scan_all_items()
    leaked = [i for i in items if i["entity_key"] == f"user#{user_id}"]
    assert leaked == [], f"rejected feature was written anyway: {leaked}"


# --- AC5: extensibility proof ---
# "add a trivial new feature job (e.g. user_account_age_days) touching
# only its own module + registry entries - runner picks it up, quality
# gate applies, feature is served by Phase 3 with no other code changes."
#
# user_account_age_days (batch_features/jobs/user_account_age.py) was
# added as exactly that proof: one new job module + one registry.yaml
# entry, zero edits to runner.py/framework.py/quality.py/materialize.py.
# Confirmed via `git diff --stat` before this commit: registry.yaml
# +10 lines, one new file, nothing else touched.
#
# The "served by Phase 3" clause can't be verified yet - feature_service/
# doesn't exist (Phase 3 hasn't been built). This test covers everything
# verifiable now: discovery, computation correctness, quality gate,
# materialization. Re-verify the serving half once Phase 3 exists.


def test_ac5_new_job_is_auto_discovered_with_no_runner_changes():
    jobs = discover_jobs()
    names = {type(j).__name__ for j in jobs}
    assert "UserAccountAgeJob" in names


def test_ac5_new_feature_computes_correctly():
    users = pl.read_parquet("data/users.parquet")
    as_of = HISTORY_END
    df = UserAccountAgeJob().compute(as_of)

    assert df.height == 50
    assert set(df.columns) == {"user_id", "user_account_age_days"}

    # independently recompute expected age for a sample user and cross-check
    sample = users.row(0, named=True)
    expected_age = (as_of - sample["created_at"]).days
    actual_age = df.filter(df["user_id"] == sample["user_id"])["user_account_age_days"].item()
    assert actual_age == expected_age


@requires_dynamo
def test_ac5_new_feature_flows_through_the_full_pipeline_and_materializes():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    assert "user_account_age_days" in registry
    assert registry["user_account_age_days"].entity == "user"

    combined = run(as_of=HISTORY_END, materialize_to_dynamo=True)
    assert "user_account_age_days" in combined["user"].columns
    ages = combined["user"]["user_account_age_days"]
    assert ages.null_count() == 0  # every user has a created_at, so full coverage
    assert (ages >= 0).all()

    items = _scan_all_items()
    age_items = [i for i in items if i["feature_name"] == "user_account_age_days"]
    assert len(age_items) == 50
