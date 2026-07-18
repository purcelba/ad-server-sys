"""Materialization tests against real dynamodb-local — require `make up`.
Uses a per-test-run unique table name so tests don't collide with a
concurrently-running `make features` and don't need cleanup between runs
(dynamodb-local is in-memory, reset on container restart)."""

import datetime as dt
import uuid
from decimal import Decimal

import polars as pl
import pytest

from adserver.batch_features import materialize


@pytest.fixture
def table_name(monkeypatch):
    name = f"features-test-{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(materialize, "TABLE_NAME", name)
    yield name


def _dynamo_reachable() -> bool:
    try:
        materialize.get_resource().meta.client.list_tables(Limit=1)
        return True
    except Exception:
        return False


requires_dynamo = pytest.mark.skipif(not _dynamo_reachable(), reason="dynamodb-local not reachable — run `make up`")


@requires_dynamo
def test_create_table_is_idempotent(table_name):
    materialize.create_table_if_not_exists()
    materialize.create_table_if_not_exists()  # must not raise
    tables = {t.name for t in materialize.get_resource().tables.all()}
    assert table_name in tables


@requires_dynamo
def test_materialize_writes_all_value_types(table_name):
    df = pl.DataFrame(
        {
            "user_id": ["u_test_1"],
            "user_ctr_30d": [0.0512],
            "user_impressions_7d": [42],
            "audience_memberships": [["weekday_commuters"]],
        }
    )
    computed_at = dt.datetime(2026, 7, 18, tzinfo=dt.timezone.utc)
    count = materialize.materialize(
        "user",
        df,
        ["user_ctr_30d", "user_impressions_7d", "audience_memberships"],
        computed_at,
    )
    assert count == 3

    resource = materialize.get_resource()
    table = resource.Table(table_name)
    resp = table.query(
        KeyConditionExpression="entity_key = :ek",
        ExpressionAttributeValues={":ek": "user#u_test_1"},
    )
    items = {item["feature_name"]: item for item in resp["Items"]}
    assert items["user_ctr_30d"]["value"] == Decimal("0.0512")
    assert items["user_impressions_7d"]["value"] == Decimal("42")
    assert items["audience_memberships"]["value"] == ["weekday_commuters"]
    assert items["user_ctr_30d"]["computed_at"] == computed_at.isoformat()


@requires_dynamo
def test_materialize_skips_null_values(table_name):
    df = pl.DataFrame(
        {"user_id": ["u_test_2"], "user_ctr_30d": [None]},
        schema={"user_id": pl.Utf8, "user_ctr_30d": pl.Float64},
    )
    computed_at = dt.datetime.now(dt.timezone.utc)
    count = materialize.materialize("user", df, ["user_ctr_30d"], computed_at)
    assert count == 0


@requires_dynamo
def test_materialize_map_dtype_ad_entity(table_name):
    df = pl.DataFrame({"campaign_id": ["c_test_1"], "ad_ctr_7d": [0.03]})
    computed_at = dt.datetime.now(dt.timezone.utc)
    materialize.materialize("ad", df, ["ad_ctr_7d"], computed_at)

    resource = materialize.get_resource()
    table = resource.Table(table_name)
    resp = table.get_item(Key={"entity_key": "ad#c_test_1", "feature_name": "ad_ctr_7d"})
    assert resp["Item"]["value"] == Decimal("0.03")
