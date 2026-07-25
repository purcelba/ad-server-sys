"""Feature retrieval service: one governed POST /features assembling
batch (DynamoDB-local) and real-time (Redis) features behind a single
API, per the registry's declared entities/dtypes/freshness SLAs.

Sync `def` endpoints (not `async def`) so FastAPI dispatches to its
thread pool — consistent with the rest of the codebase's sync boto3/
redis clients (materialize.py, consumer.py, publish_api.py).
"""

from __future__ import annotations

import time
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from adserver.batch_features.materialize import create_table_if_not_exists
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH
from adserver.common.registry import load_registry
from adserver.feature_service.metrics import Metrics
from adserver.feature_service.resolver import (
    ResolverError,
    get_dynamo_table,
    get_redis_client,
)
from adserver.feature_service.resolver import resolve_query as _resolve_query

HTTP_PORT = 8003


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
    app = FastAPI()
    registry = load_registry(registry_path)
    redis_client = get_redis_client()
    create_table_if_not_exists()
    dynamo_table = get_dynamo_table()
    metrics = Metrics()

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
                            q.entity_type, q.entity_id, q.feature_names, registry, redis_client, dynamo_table
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
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()
