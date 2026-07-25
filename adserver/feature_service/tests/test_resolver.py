"""Resolver tests against real Redis + DynamoDB-local — require `make up`."""

import datetime as dt
import json
import shutil
import uuid
from decimal import Decimal

import pytest

from adserver.batch_features.materialize import create_table_if_not_exists
from adserver.common.registry import load_registry
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH
from adserver.feature_service.resolver import (
    ResolverError,
    get_dynamo_table,
    get_redis_client,
    resolve_feature,
    resolve_query,
)
from adserver.stream_features.framework import SESSION_TTL_SECONDS

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        get_redis_client().ping()
        create_table_if_not_exists()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(not _infra_reachable(), reason="redis/dynamodb-local not reachable — run `make up`")


@pytest.fixture
def registry():
    return load_registry(DEFAULT_REGISTRY_PATH)


@pytest.fixture
def redis_client():
    return get_redis_client()


@pytest.fixture
def dynamo_table():
    create_table_if_not_exists()
    return get_dynamo_table()


@pytest.fixture
def user_id():
    return f"resolvertest_{uuid.uuid4().hex[:8]}"


@requires_infra
def test_resolve_real_time_feature_fresh_from_redis(registry, redis_client, dynamo_table, user_id):
    key = f"feature:user:{user_id}:user_current_ride_type"
    redis_client.set(key, json.dumps("premium"), ex=SESSION_TTL_SECONDS)

    result = resolve_feature(
        "user", user_id, "user_current_ride_type", registry["user_current_ride_type"], redis_client, dynamo_table
    )
    assert result.value == "premium"
    assert result.freshness_status == "fresh"  # just written, well within the 5s SLA
    assert result.default_substituted is False

    redis_client.delete(key)


@requires_infra
def test_resolve_real_time_feature_stale_when_old(registry, redis_client, dynamo_table, user_id):
    key = f"feature:user:{user_id}:user_current_ride_type"
    # simulate an old write: TTL far below the full 1800s means a large inferred age
    redis_client.set(key, json.dumps("standard"), ex=100)

    result = resolve_feature(
        "user", user_id, "user_current_ride_type", registry["user_current_ride_type"], redis_client, dynamo_table
    )
    assert result.value == "standard"
    assert result.freshness_status == "stale"  # inferred age (1800-100=1700s) >> 5s SLA

    redis_client.delete(key)


@requires_infra
def test_resolve_batch_feature_from_dynamo(registry, redis_client, dynamo_table, user_id):
    now = dt.datetime.now(dt.timezone.utc)
    dynamo_table.put_item(
        Item={
            "entity_key": f"user#{user_id}",
            "feature_name": "user_ctr_30d",
            "value": Decimal("0.05"),
            "computed_at": now.isoformat(),
        }
    )

    result = resolve_feature("user", user_id, "user_ctr_30d", registry["user_ctr_30d"], redis_client, dynamo_table)
    assert result.value == 0.05
    assert result.freshness_status == "fresh"
    assert result.default_substituted is False

    dynamo_table.delete_item(Key={"entity_key": f"user#{user_id}", "feature_name": "user_ctr_30d"})


@requires_infra
def test_resolve_missing_feature_substitutes_default(registry, redis_client, dynamo_table, user_id):
    result = resolve_feature("user", user_id, "user_ctr_30d", registry["user_ctr_30d"], redis_client, dynamo_table)
    assert result.value == registry["user_ctr_30d"].default
    assert result.freshness_status == "missing"
    assert result.default_substituted is True


@requires_infra
def test_resolve_never_returns_null_for_missing(registry, redis_client, dynamo_table, user_id):
    result = resolve_feature(
        "user", user_id, "audience_memberships", registry["audience_memberships"], redis_client, dynamo_table
    )
    assert result.value is not None
    assert result.value == []  # the registry default, not null


@requires_infra
def test_resolve_query_rejects_unregistered_feature(registry, redis_client, dynamo_table, user_id):
    with pytest.raises(ResolverError, match="not_a_real_feature"):
        resolve_query("user", user_id, ["not_a_real_feature"], registry, redis_client, dynamo_table)


@requires_infra
def test_resolve_query_rejects_entity_mismatch(registry, redis_client, dynamo_table, user_id):
    with pytest.raises(ResolverError, match="ad_ctr_7d"):
        resolve_query("user", user_id, ["ad_ctr_7d"], registry, redis_client, dynamo_table)


@requires_infra
def test_resolve_query_mixes_batch_and_real_time_and_missing(registry, redis_client, dynamo_table, user_id):
    key = f"feature:user:{user_id}:user_current_ride_type"
    redis_client.set(key, json.dumps("shared"), ex=SESSION_TTL_SECONDS)
    dynamo_table.put_item(
        Item={
            "entity_key": f"user#{user_id}",
            "feature_name": "user_ctr_30d",
            "value": Decimal("0.03"),
            "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
    )

    results = resolve_query(
        "user",
        user_id,
        ["user_current_ride_type", "user_ctr_30d", "user_impressions_7d"],
        registry,
        redis_client,
        dynamo_table,
    )

    assert results["user_current_ride_type"].value == "shared"
    assert results["user_current_ride_type"].freshness_status == "fresh"
    assert results["user_ctr_30d"].value == 0.03
    assert results["user_impressions_7d"].freshness_status == "missing"

    redis_client.delete(key)
    dynamo_table.delete_item(Key={"entity_key": f"user#{user_id}", "feature_name": "user_ctr_30d"})
