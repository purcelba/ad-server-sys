"""Tab 2: Ops - request rate, p99 latency by stage, fallback-rung counts,
consumer lag, spend/delivery by campaign, and experiment arm split, all
polled live from each service's `/metrics` and the decision log. This is
what makes Phase 5's degradation ladder and Phase 6's measurement tools
visible without reading logs (AC2).

Every panel degrades to "unreachable" independently rather than crashing
the whole tab - the Ops tab specifically needs to survive the same
failure scenarios (feature_service down, bidder down, Redis down) it's
supposed to be showing.

Spend/delivery and arm-split panels reuse `ops.reconcile.reconcile()` and
`ops.readout.per_arm_ctr()` directly rather than recomputing either -
`ops/` is batch-tooling library code (the same category `publish_api.py`
already imports `datagen.replay` from), not another service to call over
HTTP.

**Polling uses one persistent, keep-alive `httpx.Client`, not a fresh
connection per poll - discovered necessary, not a style choice.**
`adserver/adserver/service.py` runs multiple uvicorn worker processes
(`workers=WORKER_COUNT`), each with its own independent in-memory
`Metrics()` instance (no shared/aggregated metrics store - consistent
with this project's "keep it inspectable with curl, no Prometheus stack"
convention). A one-shot `httpx.get()` opens a new TCP connection each
call, which the OS distributes across workers roughly at random -
confirmed directly: repeated one-shot polls during a live test returned
wildly different `request_count` values (0, 2, 148...) poll to poll, one
call per worker. A single persistent client's connection stays pinned to
one worker for its keep-alive lifetime (also confirmed directly: 8
consecutive polls over one client returned the identical value every
time) - so every number this tab shows is real and internally
consistent (rising counts, shifting latency), just scoped to whichever
one worker its connection landed on rather than every worker's
aggregate. Documented here because it's a real, load-bearing reason for
this design, not obvious from the code alone.
"""

from __future__ import annotations

import random
import time
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import redis
import streamlit as st

from adserver.adserver.decision_log import read_decisions
from adserver.ops.reconcile import reconcile
from adserver.ops.readout import per_arm_ctr
from adserver.ui.logic import campaign_label, compute_rate

DATA_DIR = Path("data")
REFRESH_INTERVAL_S = 2.0

METRICS_URLS = {
    "stream_features_consumer": "http://localhost:8001/metrics",
    "adserver": "http://localhost:8005/metrics",
}


@st.cache_data(ttl=30)
def _load_campaigns() -> pl.DataFrame:
    return pl.read_parquet(DATA_DIR / "campaigns.parquet")


@st.cache_data(ttl=30)
def _load_users_by_id() -> dict[str, dict[str, Any]]:
    return {r["user_id"]: r for r in pl.read_parquet(DATA_DIR / "users.parquet").to_dicts()}


@st.cache_resource
def _metrics_client() -> httpx.Client:
    return httpx.Client()


def _poll(url: str) -> dict[str, Any] | None:
    try:
        resp = _metrics_client().get(url, timeout=2.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError:
        return None


def _render_request_rate_and_latency(adserver_metrics: dict[str, Any] | None) -> None:
    st.subheader("Request rate + p99 latency by stage")
    if adserver_metrics is None:
        st.warning("adserver /metrics unreachable")
        return

    prev = st.session_state.get("ops_prev_adserver")
    now = time.time()
    if prev is not None:
        rate = compute_rate(prev["request_count"], adserver_metrics["request_count"], now - prev["ts"])
        st.metric("Requests/sec", f"{rate:.1f}")
    else:
        st.caption("Requests/sec: waiting for a second sample...")
    st.session_state["ops_prev_adserver"] = {"request_count": adserver_metrics["request_count"], "ts": now}

    st.table(
        [
            {"stage": stage, **{k: round(v, 2) for k, v in stats.items()}}
            for stage, stats in adserver_metrics.get("stage_latency_ms", {}).items()
        ]
    )


def _render_fallback_counts(adserver_metrics: dict[str, Any] | None) -> None:
    st.subheader("Fallback-rung counts")
    if adserver_metrics is None:
        st.warning("adserver /metrics unreachable")
        return
    counts = adserver_metrics.get("fallback_rung_counts", {})
    if not counts:
        st.caption("No fallbacks fired yet.")
        return
    st.bar_chart(counts)


def _render_consumer_lag(consumer_metrics: dict[str, Any] | None) -> None:
    st.subheader("Consumer lag")
    if consumer_metrics is None:
        st.warning("stream_features consumer /metrics unreachable")
        return
    st.metric("Lag (s)", f"{consumer_metrics.get('lag_seconds', 0.0):.2f}")
    st.caption(f"{consumer_metrics.get('events_consumed_total', 0)} events consumed total")


def _render_spend_delivery(decisions: list[dict[str, Any]]) -> None:
    st.subheader("Spend/delivery by campaign")
    try:
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        campaigns = _load_campaigns()
        rows = reconcile(campaigns, decisions, redis_client)
    except redis.RedisError:
        st.warning("Redis unreachable")
        return
    if not rows:
        st.caption("No campaigns with pacing activity yet.")
        return
    campaigns_by_id = {r["campaign_id"]: r for r in campaigns.to_dicts()}
    st.dataframe(
        [
            {
                "campaign": campaign_label(r["campaign_id"], campaigns_by_id),
                "type": r["demand_type"],
                "remaining": round(r["remaining_capacity"], 1),
                "expected_consumed": round(r["expected_consumed"], 1),
                "actual_served": r["actual_served"],
                "discrepancy": round(r["discrepancy"], 1),
            }
            for r in rows
        ]
    )


def _render_arm_split(decisions: list[dict[str, Any]]) -> None:
    st.subheader("Experiment arm split")
    users_by_id = _load_users_by_id()
    campaigns_by_id = {r["campaign_id"]: r for r in _load_campaigns().to_dicts()}
    report = per_arm_ctr(decisions, users_by_id, campaigns_by_id, random.Random(42))
    if not report:
        st.caption("No arm-attributed impressions yet.")
        return
    st.dataframe(
        [
            {
                "arm": arm,
                "impressions": r["impressions"],
                "clicks": r["clicks"],
                "ctr": round(r["ctr"], 4),
                "ci_95": f"[{r['ci_95'][0]:.4f}, {r['ci_95'][1]:.4f}]",
            }
            for arm, r in sorted(report.items())
        ]
    )


def render() -> None:
    st.header("Ops")
    st.caption(
        "Polled live from every service's /metrics plus the decision "
        "log - a Phase 5 failure scenario should show up here (rising "
        "fallback counts, shifted stage latency) without reading logs."
    )
    auto_refresh = st.checkbox("Auto-refresh (every 2s)", value=True)

    metrics = {name: _poll(url) for name, url in METRICS_URLS.items()}
    decisions = read_decisions()

    col1, col2 = st.columns(2)
    with col1:
        _render_request_rate_and_latency(metrics["adserver"])
        _render_consumer_lag(metrics["stream_features_consumer"])
    with col2:
        _render_fallback_counts(metrics["adserver"])

    st.divider()
    _render_spend_delivery(decisions)
    st.divider()
    _render_arm_split(decisions)

    if auto_refresh:
        time.sleep(REFRESH_INTERVAL_S)
        st.rerun()
