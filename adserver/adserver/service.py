"""The ad server: `POST /serve` assembles retrieval -> feature fetch ->
scoring -> external demand -> yield arbitration, with the degradation
ladder wired directly into the request handler (every dependency call is
its own try/except, falling through to the next rung rather than
propagating) — see README.md for the written SLO and the three defined
degraded modes this implements.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import gc
import multiprocessing
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio.to_thread
import httpx
import polars as pl
import redis
import threadpoolctl
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from adserver.adserver import decision_log, pacing
from adserver.adserver.experiment import ARM_VERSIONS, assign_arm
from adserver.adserver.features import FeatureFetchError, fetch_candidate_ad_features, fetch_user_features
from adserver.adserver.metrics import Metrics
from adserver.adserver.retrieval import retrieve_candidates
from adserver.adserver.scoring import score_candidates
from adserver.bidder_stub.service import HTTP_PORT as BIDDER_PORT
from adserver.common.audiences import load_audiences
from adserver.ranking.scorer import Scorer

HTTP_PORT = 8005
BIDDER_URL = f"http://localhost:{BIDDER_PORT}/bid"
AUDIENCES_PATH = Path(__file__).resolve().parents[1] / "common" / "audiences.yaml"
HOUSE_AD_CAMPAIGN_ID = "house_ad"

FEATURE_TIMEOUT_S = 0.020
BIDDER_TIMEOUT_S = 0.030

# A single scorer.score() call for v2 (HistGradientBoostingClassifier)
# measured at ~2300ms average under 50 concurrent Python threads, vs
# ~5.5ms sequential — sklearn's internal OpenMP/BLAS thread pool
# oversubscribes badly when many concurrent request-handling threads each
# try to spawn their own internal parallelism for a single-row prediction
# that doesn't need it. Limiting every thread-pooled library to 1 thread
# per worker process (real concurrency already comes from multiple
# uvicorn worker PROCESSES below, not from threads within one) fixed it:
# same benchmark measured ~23ms average, ~108ms p99 afterward.
threadpoolctl.threadpool_limits(1)

WORKER_COUNT = min(8, multiprocessing.cpu_count())

# A module-level, monkeypatchable clock — same "clock-mockable" pattern
# Phase 2's TTL test established. AC3's guaranteed-delivery test needs to
# drive a simulated 10-day flight without waiting 10 real days; tests
# monkeypatch this function rather than dt.datetime.now() directly.
_clock = dt.datetime.now


class ServeRequest(BaseModel):
    user_id: str
    session_id: str
    slot: str = "default"


class ServeResponse(BaseModel):
    request_id: str
    experiment_arm: str
    winner_campaign_id: str | None
    price: float | None
    fallback_rung: str | None


def _eligible_by_catalog_only(campaigns: pl.DataFrame, now: dt.date) -> list[dict[str, Any]]:
    """Status + flight only — no pacing, no audience check. Used only by
    the Redis-down popularity fallback, which can't reach pacing.py's
    Redis-backed capacity counters at all."""
    return [
        row
        for row in campaigns.to_dicts()
        if row["status"] == "active" and row["flight_start"] <= now <= row["flight_end"]
    ]


def _compute_popularity_ranking(http_client: httpx.Client, campaigns: pl.DataFrame) -> list[str]:
    """Ad-entity features never touch Redis (feature_service's own design
    — see its README), so this call still succeeds even when Redis is
    down. Computed once at startup ("periodically refreshed" in
    production; a fixed snapshot is enough for this project's scale) so
    the Redis-down fallback path itself never needs Redis or a live
    feature call."""
    campaign_ids = campaigns["campaign_id"].to_list()
    try:
        ad_features = fetch_candidate_ad_features(http_client, campaign_ids, timeout_s=2.0)
    except FeatureFetchError:
        return list(campaign_ids)  # feature_service itself unavailable at startup - arbitrary order, still functional

    def _ctr(cid: str) -> float:
        feature = ad_features.get(cid, {}).get("ad_ctr_30d")
        return feature.value if feature is not None else 0.0

    return sorted(campaign_ids, key=_ctr, reverse=True)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    # Starlette's default AnyIO threadpool caps sync `def` endpoints at 40
    # concurrent workers per process - under sustained load with several
    # I/O-bound stages (feature fetch, bidder call) per request, in-flight
    # requests can back up past that cap and queue, inflating whichever
    # stage's wall-clock timer happens to be running when a queued thread
    # finally gets scheduled. Same fix feature_service/service.py needed.
    anyio.to_thread.current_default_thread_limiter().total_tokens = 200
    yield


def create_app(data_dir: Path = Path("data"), bidder_url: str = BIDDER_URL) -> FastAPI:
    # A full cyclic-GC pass mid-request was Phase 3's dominant source of
    # tail latency under load for feature_service - same fix here: this
    # service allocates almost no reference cycles per request (plain
    # dicts/dataclasses/pydantic models), so raise the thresholds and
    # freeze startup allocations out of future collections.
    gc.set_threshold(50_000, 500, 500)

    app = FastAPI(lifespan=_lifespan)
    campaigns = pl.read_parquet(data_dir / "campaigns.parquet")
    campaign_by_id = {row["campaign_id"]: row for row in campaigns.to_dicts()}
    audiences = load_audiences(AUDIENCES_PATH)
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    http_client = httpx.Client()
    metrics = Metrics()

    scorers: dict[str, Scorer | None] = {}
    for version in set(ARM_VERSIONS.values()):
        try:
            scorers[version] = Scorer(version=version)
        except Exception:
            scorers[version] = None  # degrades to house ad at request time for this arm

    popularity_ranking = _compute_popularity_ranking(http_client, campaigns)

    gc.collect()
    gc.freeze()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics")
    def get_metrics():
        return metrics.snapshot()

    @app.post("/serve", response_model=ServeResponse)
    def serve(req: ServeRequest):
        request_id = str(uuid.uuid4())
        now = _clock()
        start = time.time()
        stage_latencies: dict[str, float] = {}

        def _popularity_fallback_response(rung: str, arm: str, version: str, now: dt.datetime, stage_latencies: dict[str, float], start: float) -> ServeResponse:
            catalog_eligible = {row["campaign_id"]: row for row in _eligible_by_catalog_only(campaigns, now.date())}
            winner_id = next((cid for cid in popularity_ranking if cid in catalog_eligible), None)
            stage_latencies["total"] = (time.time() - start) * 1000
            metrics.record_request(stage_latencies, fallback_rung=rung)
            decision_log.log_decision(
                {
                    "request_id": request_id,
                    "ts": now,
                    "user_id": req.user_id,
                    "experiment_arm": arm,
                    "model_version": version,
                    "winner": winner_id,
                    "price": None,
                    "rung": rung,
                    "fallback_rung": rung,
                    "stage_latencies_ms": stage_latencies,
                }
            )
            return ServeResponse(
                request_id=request_id, experiment_arm=arm, winner_campaign_id=winner_id, price=None, fallback_rung=rung
            )

        arm = assign_arm(req.user_id)
        version = ARM_VERSIONS[arm]
        scorer = scorers.get(version)

        # --- rung 3: model load failure -> house ad, no other stage needed ---
        if scorer is None:
            fallback_rung = "model_load_failure_house_ad"
            stage_latencies["total"] = (time.time() - start) * 1000
            metrics.record_request(stage_latencies, fallback_rung=fallback_rung)
            decision_log.log_decision(
                {
                    "request_id": request_id,
                    "ts": now,
                    "user_id": req.user_id,
                    "experiment_arm": arm,
                    "model_version": version,
                    "winner": HOUSE_AD_CAMPAIGN_ID,
                    "price": None,
                    "rung": fallback_rung,
                    "fallback_rung": fallback_rung,
                }
            )
            return ServeResponse(
                request_id=request_id,
                experiment_arm=arm,
                winner_campaign_id=HOUSE_AD_CAMPAIGN_ID,
                price=None,
                fallback_rung=fallback_rung,
            )

        # --- user features (needed early, for retrieval's audience check) ---
        # rung 2: feature_service itself unreachable/times out (the whole
        # call, not one feature) -> cached popularity ranking, skipping
        # personalized scoring entirely for this request.
        t0 = time.time()
        try:
            user_features_raw = fetch_user_features(http_client, req.user_id, timeout_s=FEATURE_TIMEOUT_S)
            user_features = {name: fv.value for name, fv in user_features_raw.items()}
        except FeatureFetchError:
            stage_latencies["features"] = (time.time() - t0) * 1000
            return _popularity_fallback_response(
                "feature_service_down_popularity_fallback", arm, version, now, stage_latencies, start
            )
        stage_latencies["features"] = (time.time() - t0) * 1000
        audience_memberships = user_features.get("audience_memberships") or []

        # --- retrieval (status/flight/pacing/audience) ---
        # rung 2 also fires here: Redis (pacing) unreachable -> same
        # cached popularity fallback, since retrieval can't judge
        # remaining budget/goal capacity without it.
        t0 = time.time()
        try:
            retrieval = retrieve_candidates(
                campaigns,
                now.date(),
                audience_memberships,
                audiences,
                remaining_capacity=lambda row: pacing.get_remaining_capacity(redis_client, row),
            )
            eligible = retrieval.eligible
            audience_exclusions = retrieval.excluded_by_audience
        except redis.RedisError:
            stage_latencies["retrieval"] = (time.time() - t0) * 1000
            return _popularity_fallback_response(
                "redis_down_popularity_fallback", arm, version, now, stage_latencies, start
            )
        stage_latencies["retrieval"] = (time.time() - t0) * 1000

        auction_candidates = [c for c in eligible if c["demand_type"] == "auction"]
        guaranteed_candidates = [c for c in eligible if c["demand_type"] == "guaranteed"]

        # --- ad features for eligible auction candidates ---
        t0 = time.time()
        try:
            ad_features_raw = fetch_candidate_ad_features(
                http_client, [c["campaign_id"] for c in auction_candidates], timeout_s=FEATURE_TIMEOUT_S
            )
        except FeatureFetchError:
            stage_latencies["features"] += (time.time() - t0) * 1000
            return _popularity_fallback_response(
                "feature_service_down_popularity_fallback", arm, version, now, stage_latencies, start
            )
        stage_latencies["features"] += (time.time() - t0) * 1000

        # --- scoring ---
        t0 = time.time()
        scored_auction = score_candidates(scorer, user_features_raw, auction_candidates, ad_features_raw, audiences, now)
        stage_latencies["scoring"] = (time.time() - t0) * 1000

        # --- external bidder, hard 30ms timeout ---
        t0 = time.time()
        external_bid: float | None = None
        external_bid_outcome = "no_bid"
        try:
            resp = http_client.post(
                bidder_url,
                json={"request_id": request_id, "user_id": req.user_id, "slot": req.slot},
                timeout=BIDDER_TIMEOUT_S,
            )
            resp.raise_for_status()
            external_bid = resp.json()["bid"]
            external_bid_outcome = "bid"
        except httpx.HTTPError:
            external_bid = None
            external_bid_outcome = "timeout_or_error"
        stage_latencies["bidder"] = (time.time() - t0) * 1000

        # --- yield arbitration ---
        delivered_by_campaign = {
            c["campaign_id"]: pacing.get_delivered(redis_client, c) for c in guaranteed_candidates
        }
        arbitration = pacing.arbitrate(guaranteed_candidates, scored_auction, external_bid, now.date(), delivered_by_campaign)

        if arbitration.winner_campaign_id:
            winner_row = campaign_by_id[arbitration.winner_campaign_id]
            pacing.decrement_capacity(redis_client, winner_row)
            if arbitration.rung == "guaranteed":
                pacing.record_delivery(redis_client, winner_row)

        stage_latencies["total"] = (time.time() - start) * 1000
        metrics.record_request(stage_latencies)

        decision_log.log_decision(
            {
                "request_id": request_id,
                "ts": now,
                "user_id": req.user_id,
                "experiment_arm": arm,
                "model_version": version,
                "candidate_set": [c["campaign_id"] for c in eligible],
                "audience_exclusions": [dataclasses.asdict(e) for e in audience_exclusions],
                "scores": [
                    {"campaign_id": s.campaign_id, "pctr": s.pctr, "ecpm": s.ecpm} for s in scored_auction
                ],
                "external_bid_outcome": external_bid_outcome,
                "external_bid": external_bid,
                "winner": arbitration.winner_campaign_id,
                "price": arbitration.price,
                "rung": arbitration.rung,
                "fallback_rung": None,
                "stage_latencies_ms": stage_latencies,
            }
        )

        return ServeResponse(
            request_id=request_id,
            experiment_arm=arm,
            winner_campaign_id=arbitration.winner_campaign_id,
            price=arbitration.price,
            fallback_rung=None,
        )

    return app


def main() -> None:
    # factory=True: uvicorn imports this module and calls create_app()
    # itself, once per worker process (same pattern as
    # feature_service/service.py, for the same reason - a single Python
    # process's event loop does real per-request work, in this case
    # heavier than feature_service's, so it needs the same fix).
    uvicorn.run(
        "adserver.adserver.service:create_app",
        factory=True,
        host="0.0.0.0",
        port=HTTP_PORT,
        workers=WORKER_COUNT,
        access_log=False,
    )


if __name__ == "__main__":
    main()
