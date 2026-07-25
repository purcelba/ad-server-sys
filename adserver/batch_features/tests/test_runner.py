import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from adserver.batch_features.framework import FeatureJob
from adserver.batch_features.quality import QualityGateError
from adserver.batch_features.runner import (
    DEFAULT_REGISTRY_PATH,
    RunnerError,
    _validate_output,
    discover_jobs,
    run,
)
from adserver.common.registry import load_registry

AS_OF = dt.date(2026, 7, 18)

EXPECTED_JOB_CLASSES = {
    "AdCtr7dJob",
    "AdCtr30dJob",
    "AdImpressionsJob",
    "CampaignSpendYesterdayJob",
    "UserCtrJob",
    "UserCtrByCategoryJob",
    "UserImpressionsJob",
    "UserRidesPerWeekJob",
}


def test_discover_jobs_finds_all_expected():
    names = {type(j).__name__ for j in discover_jobs()}
    assert EXPECTED_JOB_CLASSES.issubset(names)


def test_run_produces_expected_shapes_and_columns(tmp_path):
    output_dir = tmp_path / "features"
    combined = run(as_of=AS_OF, output_dir=output_dir)

    assert combined["user"].height == 50
    assert combined["ad"].height == 40
    user_cols = set(combined["user"].columns)
    assert {
        "user_id",
        "user_ctr_30d",
        "user_ctr_by_category_30d",
        "user_impressions_7d",
        "user_rides_per_week",
    }.issubset(user_cols)
    ad_cols = set(combined["ad"].columns)
    assert {
        "campaign_id",
        "ad_ctr_7d",
        "ad_ctr_30d",
        "ad_impressions_7d",
        "campaign_spend_yesterday",
    }.issubset(ad_cols)


def test_run_writes_partitioned_offline_parquet(tmp_path):
    output_dir = tmp_path / "features"
    run(as_of=AS_OF, output_dir=output_dir)

    user_partition = output_dir / "entity=user" / f"asof={AS_OF.isoformat()}" / "features.parquet"
    ad_partition = output_dir / "entity=ad" / f"asof={AS_OF.isoformat()}" / "features.parquet"
    assert user_partition.exists()
    assert ad_partition.exists()
    assert pl.read_parquet(user_partition).height == 50


class _BrokenColumnsJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_ctr_30d"]

    def compute(self, as_of, data_dir=Path("data")) -> pl.DataFrame:
        return pl.DataFrame({"user_id": ["u_0001"], "wrong_column_name": [0.5]})


class _UnregisteredFeatureJob(FeatureJob):
    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["not_a_real_feature"]

    def compute(self, as_of, data_dir=Path("data")) -> pl.DataFrame:
        return pl.DataFrame({"user_id": ["u_0001"], "not_a_real_feature": [1.0]})


def test_validate_output_raises_on_column_mismatch():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    job = _BrokenColumnsJob()
    df = job.compute(AS_OF)
    with pytest.raises(RunnerError, match="wrong_column_name|expected"):
        _validate_output(job, df, registry)


def test_validate_output_raises_on_unregistered_feature():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    job = _UnregisteredFeatureJob()
    df = job.compute(AS_OF)
    with pytest.raises(RunnerError, match="not_a_real_feature"):
        _validate_output(job, df, registry)


class _SparseButValidJob(FeatureJob):
    """Correct columns, registered feature - but only 1/50 users
    represented, well below the row-count gate's 80% threshold. Simulates
    what a poisoned upstream events.parquet would look like downstream."""

    def entity(self) -> str:
        return "user"

    def outputs(self) -> list[str]:
        return ["user_ctr_30d"]

    def compute(self, as_of, data_dir=Path("data")) -> pl.DataFrame:
        return pl.DataFrame({"user_id": ["u_0001"], "user_ctr_30d": [0.05]})


def test_run_blocks_materialization_when_a_job_fails_the_quality_gate(tmp_path):
    output_dir = tmp_path / "features"
    with pytest.raises(QualityGateError, match="row-count"):
        run(as_of=AS_OF, output_dir=output_dir, jobs=[_SparseButValidJob()])

    # no partial write - the failure must happen before any output is written
    assert not output_dir.exists()


class _NullIdRowJob(FeatureJob):
    """1 real entity + 1 row with a null id, mimicking what Polars
    group_by() produces when a source column (e.g. campaign_id) is
    corrupted to null - the null-keyed group is a real row in the
    DataFrame, but it doesn't represent any real entity."""

    def entity(self) -> str:
        return "ad"

    def outputs(self) -> list[str]:
        return ["ad_ctr_7d"]

    def compute(self, as_of, data_dir=Path("data")) -> pl.DataFrame:
        return pl.DataFrame(
            {"campaign_id": ["c_0001", None], "ad_ctr_7d": [0.03, 0.05]},
            schema={"campaign_id": pl.Utf8, "ad_ctr_7d": pl.Float64},
        )

    def expected_entity_count(self, as_of, data_dir=Path("data")) -> int:
        return 2  # both "entities" would count if the null row weren't dropped


def test_run_never_counts_a_null_entity_id_as_coverage(tmp_path):
    """Regression test: a null id must be dropped before the quality gate
    sees it, both so row-count reflects only real entities (1/2 = 50%,
    correctly failing) and so a literal 'ad#None' key never reaches
    materialize()."""
    output_dir = tmp_path / "features"
    with pytest.raises(QualityGateError, match="row-count"):
        run(as_of=AS_OF, output_dir=output_dir, jobs=[_NullIdRowJob()])
    assert not output_dir.exists()
