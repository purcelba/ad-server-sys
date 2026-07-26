"""bidder_stub: the fake external programmatic bidder. Not real auction
logic — a controllable dependency for exercising the ad server's own
failure-mode handling (AC2b's "bidder latency forced above timeout").
Configurable latency distribution and failure rate, both via env var
defaults and per-request query-param overrides (so a test can force
above-timeout latency for one call without restarting the process).
"""

from __future__ import annotations

import os
import random
import time

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from adserver.bidder_stub.metrics import Metrics

HTTP_PORT = 8004

DEFAULT_LATENCY_MEAN_MS = float(os.environ.get("BIDDER_LATENCY_MEAN_MS", 10.0))
DEFAULT_LATENCY_STD_MS = float(os.environ.get("BIDDER_LATENCY_STD_MS", 5.0))
DEFAULT_FAILURE_RATE = float(os.environ.get("BIDDER_FAILURE_RATE", 0.0))
BID_RANGE = (0.50, 6.00)


class BidRequest(BaseModel):
    request_id: str
    user_id: str
    slot: str = "default"


class BidResponse(BaseModel):
    bid: float
    bidder: str = "external_stub"


def _sample_latency_ms(mean_ms: float, std_ms: float) -> float:
    return max(0.0, random.gauss(mean_ms, std_ms))


def create_app() -> FastAPI:
    app = FastAPI()
    metrics = Metrics()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics")
    def get_metrics():
        return metrics.snapshot()

    @app.post("/bid", response_model=BidResponse)
    def bid(
        req: BidRequest,
        latency_mean_ms: float = Query(DEFAULT_LATENCY_MEAN_MS),
        latency_std_ms: float = Query(DEFAULT_LATENCY_STD_MS),
        failure_rate: float = Query(DEFAULT_FAILURE_RATE),
    ):
        start = time.time()
        time.sleep(_sample_latency_ms(latency_mean_ms, latency_std_ms) / 1000)

        if random.random() < failure_rate:
            metrics.record_request((time.time() - start) * 1000, error=True)
            raise HTTPException(status_code=503, detail="bidder unavailable")

        metrics.record_request((time.time() - start) * 1000)
        return BidResponse(bid=round(random.uniform(*BID_RANGE), 2))

    return app


def main() -> None:
    uvicorn.run(create_app(), host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()
