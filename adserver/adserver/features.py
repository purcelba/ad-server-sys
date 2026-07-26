"""Feature fetch over HTTP — never imports `feature_service.resolver`
directly, per the global convention ("services communicate only via HTTP,
the event stream, or the online store — never by importing each other's
code"). User features are fetched once per request (needed early, before
retrieval, for the audience-eligibility check); candidate ad features are
fetched together in one batched call for whatever survived retrieval —
the exact batching `feature_service`'s API was designed for.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

FEATURE_SERVICE_URL = "http://localhost:8003/features"

USER_FEATURE_NAMES = [
    "user_ctr_30d",
    "user_ctr_by_category_30d",
    "user_impressions_7d",
    "user_rides_per_week",
    "user_account_age_days",
    "audience_memberships",
    "user_session_active",
    "user_current_destination_category",
    "user_current_ride_type",
]
AD_FEATURE_NAMES = ["ad_ctr_7d", "ad_ctr_30d", "ad_impressions_7d", "campaign_spend_yesterday"]


class FeatureFetchError(Exception):
    """Raised on any feature_service failure — timeout, connection error,
    non-2xx response. Callers (service.py) catch this to trigger the
    degradation ladder's rung 2 (cached popularity ranking), never let it
    propagate to a 500."""


@dataclass(frozen=True)
class FeatureValue:
    """A minimal local stand-in for feature_service.resolver.FeatureResult
    — duplicated rather than imported, since importing it would mean
    importing feature_service's code directly rather than talking to it
    over HTTP. Only the fields ranking.assemble's adapters actually read."""

    value: Any
    freshness_status: str


def _post_features(client: httpx.Client, queries: list[dict[str, Any]], timeout_s: float) -> list[dict[str, Any]]:
    try:
        resp = client.post(FEATURE_SERVICE_URL, json={"queries": queries}, timeout=timeout_s)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise FeatureFetchError(str(exc)) from exc
    return resp.json()["results"]


def fetch_user_features(client: httpx.Client, user_id: str, timeout_s: float = 0.02) -> dict[str, FeatureValue]:
    results = _post_features(
        client, [{"entity_type": "user", "entity_id": user_id, "feature_names": USER_FEATURE_NAMES}], timeout_s
    )
    features = results[0]["features"]
    return {name: FeatureValue(value=f["value"], freshness_status=f["freshness_status"]) for name, f in features.items()}


def fetch_candidate_ad_features(
    client: httpx.Client, campaign_ids: list[str], timeout_s: float = 0.02
) -> dict[str, dict[str, FeatureValue]]:
    if not campaign_ids:
        return {}
    queries = [{"entity_type": "ad", "entity_id": cid, "feature_names": AD_FEATURE_NAMES} for cid in campaign_ids]
    results = _post_features(client, queries, timeout_s)
    return {
        result["entity_id"]: {
            name: FeatureValue(value=f["value"], freshness_status=f["freshness_status"])
            for name, f in result["features"].items()
        }
        for result in results
    }
