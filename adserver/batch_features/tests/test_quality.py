import polars as pl
import pytest

from adserver.batch_features.quality import QualityGateError, check


def test_passes_with_full_coverage_and_no_nulls():
    df = pl.DataFrame({"user_id": ["u1", "u2"], "feat": [0.1, 0.2]})
    check(df, expected_entity_count=2, feature_cols=["feat"], job_name="test_job")


def test_fails_on_low_row_count_ratio():
    df = pl.DataFrame({"user_id": ["u1"], "feat": [0.1]})
    with pytest.raises(QualityGateError, match="row-count"):
        check(df, expected_entity_count=10, feature_cols=["feat"], job_name="test_job")


def test_passes_at_exactly_the_threshold():
    # 8/10 = 80% = MIN_ROW_COUNT_RATIO, not below it
    df = pl.DataFrame({"user_id": [f"u{i}" for i in range(8)], "feat": [0.1] * 8})
    check(df, expected_entity_count=10, feature_cols=["feat"], job_name="test_job")


def test_fails_on_high_null_rate():
    df = pl.DataFrame(
        {"user_id": [f"u{i}" for i in range(10)], "feat": [0.1] * 4 + [None] * 6},
        schema={"user_id": pl.Utf8, "feat": pl.Float64},
    )
    with pytest.raises(QualityGateError, match="null-rate"):
        check(df, expected_entity_count=10, feature_cols=["feat"], job_name="test_job")


def test_tolerates_null_rate_at_threshold():
    # 1/20 = 5% = MAX_NULL_RATE, not above it
    df = pl.DataFrame(
        {"user_id": [f"u{i}" for i in range(20)], "feat": [0.1] * 19 + [None]},
        schema={"user_id": pl.Utf8, "feat": pl.Float64},
    )
    check(df, expected_entity_count=20, feature_cols=["feat"], job_name="test_job")


def test_zero_expected_entities_skips_row_count_check():
    df = pl.DataFrame({"user_id": [], "feat": []}, schema={"user_id": pl.Utf8, "feat": pl.Float64})
    check(df, expected_entity_count=0, feature_cols=["feat"], job_name="test_job")
