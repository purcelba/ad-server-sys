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
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import boto3
import redis
from botocore.config import Config as BotoConfig

from adserver.batch_features.materialize import (
    _KEY_PREFIX_BY_ENTITY,
    DYNAMODB_ENDPOINT,
    TABLE_NAME as DYNAMO_TABLE_NAME,
)
from adserver.common.registry import FeatureDef
from adserver.stream_features.framework import SESSION_TTL_SECONDS

REDIS_HOST = "localhost"
REDIS_PORT = 6379

# botocore's default max_pool_connections is 10 — fine for materialize.py's
# batch writes, but far too small for a serving path under concurrent load:
# under 100 concurrent requests it serialized DynamoDB calls onto 10
# connections and blew latency out to ~250ms p99 for ~5ms of real work.
DYNAMO_MAX_POOL_CONNECTIONS = 200


class ResolverError(ValueError):
    pass


class DynamoCache:
    """Small thread-safe TTL cache in front of DynamoDB-local lookups.

    Batch features are materialized at most daily (batch_features/), so a
    short in-process cache doesn't meaningfully change freshness semantics
    but avoids sending every single online request straight through to
    DynamoDB-local — whose embedded server serializes concurrent requests
    badly (measured: p99 ~60ms at 100 concurrent get_item calls, vs ~1.3ms
    uncontended), a real constraint of the local test double rather than
    of this resolver's own logic. Any real online feature store (Feast,
    Tecton, a hand-rolled service) caches in front of its batch store for
    exactly this reason — the batch side isn't built for per-request QPS.
    """

    def __init__(self, ttl_seconds: float = 2.0):
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._store: dict[tuple[str, str, str], tuple[float, Any]] = {}

    def get_or_set(self, key: tuple[str, str, str], compute):
        now = time.monotonic()
        with self._lock:
            hit = self._store.get(key)
            if hit is not None and now - hit[0] < self._ttl:
                return hit[1]
        value = compute()
        with self._lock:
            self._store[key] = (now, value)
        return value


@dataclass(frozen=True)
class FeatureResult:
    value: Any
    computed_at: str
    freshness_status: str  # "fresh" | "stale" | "missing"
    default_substituted: bool


def get_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_dynamo_table():
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=BotoConfig(max_pool_connections=DYNAMO_MAX_POOL_CONNECTIONS),
    )
    return resource.Table(DYNAMO_TABLE_NAME)


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


def _lookup_dynamo(
    dynamo_table, entity: str, entity_id: str, feature_name: str, cache: DynamoCache | None = None
) -> tuple[Any, str] | None:
    """Returns (value, computed_at_iso) or None if the item doesn't exist."""
    prefix = _KEY_PREFIX_BY_ENTITY[entity]

    def _fetch() -> tuple[Any, str] | None:
        resp = dynamo_table.get_item(Key={"entity_key": f"{prefix}#{entity_id}", "feature_name": feature_name})
        item = resp.get("Item")
        if item is None:
            return None
        return item["value"], item["computed_at"]

    if cache is None:
        return _fetch()
    return cache.get_or_set((entity, entity_id, feature_name), _fetch)


def resolve_feature(
    entity_type: str,
    entity_id: str,
    feature_name: str,
    feature_def: FeatureDef,
    redis_client: redis.Redis,
    dynamo_table,
    dynamo_cache: DynamoCache | None = None,
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

    dynamo_hit = _lookup_dynamo(dynamo_table, entity_type, entity_id, feature_name, dynamo_cache)
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
    dynamo_cache: DynamoCache | None = None,
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
        name: resolve_feature(entity_type, entity_id, name, registry[name], redis_client, dynamo_table, dynamo_cache)
        for name in feature_names
    }
