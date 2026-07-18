import datetime as dt

import polars as pl

from adserver.batch_features.jobs.audiences import AudienceMembershipsJob

AS_OF = dt.date(2026, 7, 18)


def test_schema_and_coverage():
    job = AudienceMembershipsJob()
    df = job.compute(AS_OF)
    assert df.columns == ["user_id", "audience_memberships"]
    assert df.height == 50
    assert df["audience_memberships"].dtype == pl.List(pl.Utf8)


def test_only_matching_segments_get_membership():
    """weekday_commuters requires segment == commuter; frequent_airport_travelers
    requires segment == traveler. No other segment should ever match either."""
    users = pl.read_parquet("data/users.parquet")
    df = AudienceMembershipsJob().compute(AS_OF).join(
        users.select(["user_id", "segment"]), on="user_id"
    )

    for row in df.iter_rows(named=True):
        if "weekday_commuters" in row["audience_memberships"]:
            assert row["segment"] == "commuter"
        if "frequent_airport_travelers" in row["audience_memberships"]:
            assert row["segment"] == "traveler"


def test_version_log_written(tmp_path, monkeypatch):
    log_path = tmp_path / "audience_versions.log"
    monkeypatch.setattr(
        "adserver.batch_features.jobs.audiences.VERSION_LOG_PATH", log_path
    )
    AudienceMembershipsJob().compute(AS_OF)
    assert log_path.exists()
    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 2  # two audiences currently defined
    import json

    entries = [json.loads(line) for line in lines]
    names = {e["name"] for e in entries}
    assert names == {"frequent_airport_travelers", "weekday_commuters"}
    assert all(e["definition_version"] == 1 for e in entries)
