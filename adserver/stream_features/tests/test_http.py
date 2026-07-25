from fastapi.testclient import TestClient

from adserver.stream_features.consumer import create_app
from adserver.stream_features.metrics import Metrics


def test_health_endpoint():
    client = TestClient(create_app(Metrics()))
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_metrics_endpoint_reflects_recorded_events():
    metrics = Metrics()
    metrics.record_processed("session_start", latency_ms=1.5, lag_seconds=0.2)
    metrics.record_unknown("promo_viewed")

    client = TestClient(create_app(metrics))
    resp = client.get("/metrics")
    assert resp.status_code == 200
    body = resp.json()
    assert body["events_consumed_total"] == 1
    assert body["events_consumed_by_type"] == {"session_start": 1}
    assert body["unknown_events_by_type"] == {"promo_viewed": 1}
    assert body["lag_seconds"] == 0.2
