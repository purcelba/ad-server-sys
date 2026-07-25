"""Core feature resolution: Redis first (real-time), DynamoDB-local
fallback (batch), freshness judged against the registry SLA, registry
defaults substituted when missing. Deliberately separable from FastAPI —
testable directly against real Redis/DynamoDB with no running server.

Real-time (Redis) freshness gap, resolved without touching Phase 2's
tagged consumer.py: stream_features/consumer.py writes bare JSON scalars
with a TTL, no stored computed_at. Since every write uses the same fixed
SESSION_TTL_SECONDS, age is inferred from the key's remaining TTL
(age = SESSION_TTL_SECONDS - remaining) rather than a literal timestamp —
computed_at in the response is back-calculated from that inferred age
and documented as such, not literally stored.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import redis

from adserver.batch_features.materialize import (
    _ID_COL_BY_ENTITY,
    _KEY_PREFIX_BY_ENTITY,
    TABLE_NAME as DYNAMO_TABLE_NAME,
    get_resource as get_dynamo_resource,
)
from adserver.common.registry import FeatureDef
from adserver.stream_features.framework import SESSION_TTL_SECONDS

REDIS_HOST = "localhost"
REDIS_PORT = 6379


class ResolverError(ValueError):
    pass


@dataclass(frozen=True)
class FeatureResult:
    value: Any
    computed_at: str
    freshness_status: str  # "fresh" | "stale" | "missing"
    default_substituted: bool


def get_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_dynamo_table():
    return get_dynamo_resource().Table(DYNAMO_TABLE_NAME)


def _redis_feature_key(user_id: str, feature_name: str) -> str:
    # exact key scheme stream_features/consumer.py writes - see _feature_key there
    return f"feature:user:{user_id}:{feature_name}"


def _normalize_value(value: Any, dtype: str) -> Any:
    """DynamoDB returns Decimal for numbers; JSON can't encode Decimal
    directly, so normalize per the registry's declared dtype."""
    if isinstance(value, Decimal):
        if dtype == "int":
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {k: _normalize_value(v, "float") for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_value(v, "str") for v in value]
    return value


def _lookup_redis(redis_client: redis.Redis, user_id: str, feature_name: str) -> tuple[Any, float] | None:
    """Returns (value, age_seconds) or None if the key doesn't exist."""
    key = _redis_feature_key(user_id, feature_name)
    pipe = redis_client.pipeline()
    pipe.get(key)
    pipe.pttl(key)
    raw_value, pttl_ms = pipe.execute()
    if raw_value is None or pttl_ms is None or pttl_ms < 0:
        return None
    age_seconds = SESSION_TTL_SECONDS - (pttl_ms / 1000.0)
    return json.loads(raw_value), max(age_seconds, 0.0)


def _lookup_dynamo(dynamo_table, entity: str, entity_id: str, feature_name: str) -> tuple[Any, str] | None:
    """Returns (value, computed_at_iso) or None if the item doesn't exist."""
    prefix = _KEY_PREFIX_BY_ENTITY[entity]
    resp = dynamo_table.get_item(Key={"entity_key": f"{prefix}#{entity_id}", "feature_name": feature_name})
    item = resp.get("Item")
    if item is None:
        return None
    return item["value"], item["computed_at"]


def resolve_feature(
    entity_type: str,
    entity_id: str,
    feature_name: str,
    feature_def: FeatureDef,
    redis_client: redis.Redis,
    dynamo_table,
) -> FeatureResult:
    now = dt.datetime.now(dt.timezone.utc)

    if entity_type == "user":
        redis_hit = _lookup_redis(redis_client, entity_id, feature_name)
        if redis_hit is not None:
            value, age_seconds = redis_hit
            computed_at = (now - dt.timedelta(seconds=age_seconds)).isoformat()
            status = "fresh" if age_seconds <= feature_def.freshness_sla_seconds() else "stale"
            return FeatureResult(
                value=_normalize_value(value, feature_def.dtype),
                computed_at=computed_at,
                freshness_status=status,
                default_substituted=False,
            )

    dynamo_hit = _lookup_dynamo(dynamo_table, entity_type, entity_id, feature_name)
    if dynamo_hit is not None:
        value, computed_at_iso = dynamo_hit
        computed_at = dt.datetime.fromisoformat(computed_at_iso)
        age_seconds = (now - computed_at).total_seconds()
        status = "fresh" if age_seconds <= feature_def.freshness_sla_seconds() else "stale"
        return FeatureResult(
            value=_normalize_value(value, feature_def.dtype),
            computed_at=computed_at_iso,
            freshness_status=status,
            default_substituted=False,
        )

    return FeatureResult(
        value=feature_def.default,
        computed_at=now.isoformat(),
        freshness_status="missing",
        default_substituted=True,
    )


def resolve_query(
    entity_type: str,
    entity_id: str,
    feature_names: list[str],
    registry: dict[str, FeatureDef],
    redis_client: redis.Redis,
    dynamo_table,
) -> dict[str, FeatureResult]:
    """Validates every feature_name against the registry (name exists,
    entity matches) before resolving any of them - fail the whole query
    clearly rather than silently resolving some and guessing on others."""
    for name in feature_names:
        if name not in registry:
            raise ResolverError(f"{name!r} is not a registered feature")
        if registry[name].entity != entity_type:
            raise ResolverError(
                f"{name!r} is registered as entity {registry[name].entity!r}, "
                f"not {entity_type!r}"
            )

    return {
        name: resolve_feature(entity_type, entity_id, name, registry[name], redis_client, dynamo_table)
        for name in feature_names
    }
