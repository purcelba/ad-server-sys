"""Feature retrieval service: one governed POST /features assembling
batch (DynamoDB-local) and real-time (Redis) features behind a single
API, per the registry's declared entities/dtypes/freshness SLAs.

Sync `def` endpoints (not `async def`) so FastAPI dispatches to its
thread pool — consistent with the rest of the codebase's sync boto3/
redis clients (materialize.py, consumer.py, publish_api.py).
"""

from __future__ import annotations

import gc
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

import anyio.to_thread
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from adserver.batch_features.materialize import create_table_if_not_exists
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH
from adserver.common.registry import load_registry
from adserver.feature_service.metrics import Metrics
from adserver.feature_service.resolver import (
    DynamoCache,
    ResolverError,
    get_dynamo_table,
    get_redis_client,
)
from adserver.feature_service.resolver import resolve_query as _resolve_query

HTTP_PORT = 8003

# Starlette's default AnyIO threadpool caps sync `def` endpoints at 40
# concurrent workers — under 100 concurrent requests each ~5ms of actual
# work, that queueing alone was enough to blow the 10ms p99 AC well past
# what the resolver itself takes. Raised to comfortably exceed the load
# test's concurrency.
THREADPOOL_TOKENS = 200

# A single Python ASGI process spends real time per request just parsing
# JSON, running Pydantic validation, and serializing the response on its
# one event loop thread — measured directly: even the trivial /health
# endpoint (no Redis/DynamoDB work at all) hit ~60ms p99 at 100 truly
# concurrent requests against a single worker. That's inherent to a
# single process, not this service's resolution logic — real deployments
# handle it the same way: multiple worker processes behind one port.
WORKER_COUNT = min(8, multiprocessing.cpu_count())

# redis-py's connection pool grows lazily, one connection per concurrent
# demand — the first burst of concurrent requests each pay a fresh TCP
# connect to Redis instead of reusing a warm one. Pre-opening this many
# connections per worker at startup (well above what THREADPOOL_TOKENS
# could put in flight against Redis at once within a worker) moves that
# cost out of the request path.
REDIS_POOL_WARMUP = 50


class FeatureQuery(BaseModel):
    entity_type: str
    entity_id: str
    feature_names: list[str]


class FeaturesRequest(BaseModel):
    queries: list[FeatureQuery]


class FeatureResultOut(BaseModel):
    value: object
    computed_at: str
    freshness_status: str
    default_substituted: bool


class EntityResultOut(BaseModel):
    entity_type: str
    entity_id: str
    features: dict[str, FeatureResultOut]


class FeaturesResponse(BaseModel):
    results: list[EntityResultOut]


def create_app(registry_path: Path = DEFAULT_REGISTRY_PATH) -> FastAPI:
    # a full cyclic-GC pass mid-request was the dominant source of tail
    # latency under load (a sharp jump at the ~98th percentile, otherwise
    # a flat distribution) — this service allocates almost no reference
    # cycles per request (plain dicts/dataclasses/pydantic models), so
    # gen2 collection has nothing to usefully find; raising the
    # thresholds trades a larger resident-set ceiling for a flatter tail.
    gc.set_threshold(50_000, 500, 500)

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        # only takes effect once an event loop is running (test clients
        # that never start uvicorn's loop simply keep the anyio default)
        anyio.to_thread.current_default_thread_limiter().total_tokens = THREADPOOL_TOKENS
        yield

    app = FastAPI(lifespan=_lifespan)
    registry = load_registry(registry_path)
    redis_client = get_redis_client()
    create_table_if_not_exists()
    dynamo_table = get_dynamo_table()
    dynamo_cache = DynamoCache()
    metrics = Metrics()

    with ThreadPoolExecutor(max_workers=REDIS_POOL_WARMUP) as warmup_pool:
        list(warmup_pool.map(lambda _: redis_client.ping(), range(REDIS_POOL_WARMUP)))

    # freeze everything allocated during startup (registry, clients, the
    # FastAPI app itself) into gen2 so future collections never re-scan
    # it — pairs with the raised thresholds above to keep steady-state
    # per-request GC work close to zero.
    gc.collect()
    gc.freeze()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics")
    def get_metrics():
        return metrics.snapshot()

    @app.post("/features", response_model=FeaturesResponse)
    def get_features(req: FeaturesRequest):
        start = time.time()
        try:
            results = [
                EntityResultOut(
                    entity_type=q.entity_type,
                    entity_id=q.entity_id,
                    features={
                        name: FeatureResultOut(
                            value=result.value,
                            computed_at=result.computed_at,
                            freshness_status=result.freshness_status,
                            default_substituted=result.default_substituted,
                        )
                        for name, result in _resolve_query(
                            q.entity_type,
                            q.entity_id,
                            q.feature_names,
                            registry,
                            redis_client,
                            dynamo_table,
                            dynamo_cache,
                        ).items()
                    },
                )
                for q in req.queries
            ]
        except ResolverError as exc:
            metrics.record_request((time.time() - start) * 1000, error=True)
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        metrics.record_request((time.time() - start) * 1000)
        return FeaturesResponse(results=results)

    return app


def main() -> None:
    # factory=True: uvicorn imports this module and calls create_app()
    # itself, once per worker process, rather than us building one app
    # object up front and trying to fork/share it — each worker gets its
    # own registry/redis/dynamo clients and its own dynamo_cache/metrics.
    uvicorn.run(
        "adserver.feature_service.service:create_app",
        factory=True,
        host="0.0.0.0",
        port=HTTP_PORT,
        workers=WORKER_COUNT,
        # per-request access logging is a synchronous stdout write on the
        # event loop thread — under concurrent load it was a measurable
        # chunk of the tail latency for no benefit in this local setting.
        access_log=False,
    )


if __name__ == "__main__":
    main()
