"""Tab 1: Rider - the Phase 2 mini page's successor. Fires a real session
event through the exact same publish endpoint `mini.html` uses
(`POST :8002/events` - no Kafka producer code duplicated here), then
calls `/serve` and renders the full decision-log trail: candidates,
scores, arbitration outcome, fallback rung, and per-stage latency.

**Freshness badges come from a direct `feature_service` call, not the
decision log.** `scoring.py`'s `from_online_result()` unwraps
`FeatureValue.value` before merging into what gets logged - the
`freshness_status` that made it *fresh* or *stale* is dropped from
`decision_log.jsonl`. `fetch_user_features()` (the same HTTP-client
helper `service.py` itself uses) is the only place that metadata still
exists, so this tab calls it directly for display.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import streamlit as st

from adserver.adserver.decision_log import read_decisions
from adserver.adserver.features import fetch_user_features
from adserver.ui.logic import campaign_label, find_decision

PUBLISH_URL = "http://localhost:8002/events"
SERVE_URL = "http://localhost:8005/serve"
DATA_DIR = Path("data")

# A debug UI isn't under the ad server's own 20ms feature-fetch budget -
# generous enough that a slow feature_service shows up as a visible wait,
# not a spurious failure.
UI_FEATURE_TIMEOUT_S = 2.0

EVENT_TYPES = ["session_start", "destination_entered", "ride_type_selected", "app_screen_view"]
CATEGORIES = ["food", "retail", "entertainment", "travel", "transit"]
RIDE_TYPES = ["standard", "shared", "premium"]
SCREENS = ["home", "search", "trip_history", "account", "promotions"]


@st.cache_data(ttl=30)
def _load_users() -> pl.DataFrame:
    return pl.read_parquet(DATA_DIR / "users.parquet")


@st.cache_data(ttl=30)
def _load_campaigns() -> pl.DataFrame:
    return pl.read_parquet(DATA_DIR / "campaigns.parquet")


def _fire_event(user_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    resp = httpx.post(
        PUBLISH_URL, json={"user_id": user_id, "event_type": event_type, "payload": payload}, timeout=5.0
    )
    resp.raise_for_status()
    return resp.json()


def _serve(user_id: str) -> dict[str, Any]:
    resp = httpx.post(
        SERVE_URL,
        json={"user_id": user_id, "session_id": str(uuid.uuid4()), "slot": "debug_ui"},
        timeout=5.0,
    )
    resp.raise_for_status()
    return resp.json()


def _render_serve_result(label: str, resp: dict[str, Any], campaigns_by_id: dict[str, Any]) -> None:
    decision = find_decision(read_decisions(), resp["request_id"])

    st.markdown(f"**{label}**")
    st.write(
        {
            "winner": campaign_label(resp["winner_campaign_id"], campaigns_by_id),
            "price": resp["price"],
            "experiment_arm": resp.get("experiment_arm"),
            "fallback_rung": resp.get("fallback_rung"),
        }
    )
    if decision is None:
        st.caption("decision log entry not found yet - the log write may still be in flight, try again")
        return

    if decision.get("scores"):
        st.caption("Candidates")
        st.table(
            [
                {
                    "campaign": campaign_label(c["campaign_id"], campaigns_by_id),
                    "pctr": round(c["pctr"], 4),
                    "ecpm": round(c["ecpm"], 4) if c["ecpm"] is not None else None,
                }
                for c in decision["scores"]
            ]
        )

    st.caption("Arbitration")
    st.write(
        {
            "rung": decision.get("rung"),
            "external_bid_outcome": decision.get("external_bid_outcome"),
            "external_bid": decision.get("external_bid"),
        }
    )

    if decision.get("stage_latencies_ms"):
        st.caption("Stage latencies (ms)")
        st.write({k: round(v, 2) for k, v in decision["stage_latencies_ms"].items()})


def render() -> None:
    st.header("Rider")
    st.caption(
        "Pick a user, fire a real session event through the same publish "
        "endpoint mini.html uses, then call /serve and watch the debug "
        "trail: candidates, scores, arbitration, fallback rung, and "
        "per-stage latency."
    )

    users = _load_users()
    campaigns_by_id = {row["campaign_id"]: row for row in _load_campaigns().to_dicts()}

    user_rows = users.sort("user_id").to_dicts()
    user_options = {f"{row['user_id']} ({row['segment']})": row["user_id"] for row in user_rows}
    picked_label = st.selectbox("User", list(user_options.keys()))
    user_id = user_options[picked_label]

    event_type = st.selectbox("Event type", EVENT_TYPES)
    payload: dict[str, Any] = {}
    if event_type == "destination_entered":
        payload = {"category": st.selectbox("Destination category", CATEGORIES), "geo": "sea"}
    elif event_type == "ride_type_selected":
        payload = {"ride_type": st.selectbox("Ride type", RIDE_TYPES)}
    elif event_type == "app_screen_view":
        payload = {"screen": st.selectbox("Screen", SCREENS)}

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Serve ad (before)"):
            try:
                st.session_state["rider_before"] = _serve(user_id)
            except httpx.HTTPError as exc:
                st.error(f"/serve failed: {exc}")
    with col2:
        if st.button("Fire event, then serve ad (after)"):
            try:
                _fire_event(user_id, event_type, payload)
                st.session_state["rider_after"] = _serve(user_id)
            except httpx.HTTPError as exc:
                st.error(f"request failed: {exc}")

    before = st.session_state.get("rider_before")
    after = st.session_state.get("rider_after")
    if before or after:
        col1, col2 = st.columns(2)
        with col1:
            if before:
                _render_serve_result("Before", before, campaigns_by_id)
        with col2:
            if after:
                _render_serve_result("After", after, campaigns_by_id)

    st.divider()
    st.subheader("Current user feature freshness")
    st.caption("Not from the decision log - a live feature_service call, the only place freshness_status survives.")
    if st.button("Fetch current user features"):
        with httpx.Client() as client:
            try:
                features = fetch_user_features(client, user_id, timeout_s=UI_FEATURE_TIMEOUT_S)
                st.table(
                    [
                        {"feature": name, "value": str(fv.value), "freshness": fv.freshness_status}
                        for name, fv in features.items()
                    ]
                )
            except Exception as exc:  # a debug panel, not the serving path - degrade, don't crash the tab
                st.error(f"feature fetch failed: {exc}")
