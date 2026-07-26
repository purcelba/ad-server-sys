import time

from fastapi.testclient import TestClient

from adserver.bidder_stub.service import BID_RANGE, create_app


def test_health():
    client = TestClient(create_app())
    assert client.get("/health").json() == {"status": "ok"}


def test_bid_returns_a_value_in_range():
    client = TestClient(create_app())
    resp = client.post(
        "/bid",
        json={"request_id": "r1", "user_id": "u_0001"},
        params={"latency_mean_ms": 0, "latency_std_ms": 0},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert BID_RANGE[0] <= body["bid"] <= BID_RANGE[1]
    assert body["bidder"] == "external_stub"


def test_failure_rate_one_always_fails():
    client = TestClient(create_app())
    resp = client.post(
        "/bid",
        json={"request_id": "r1", "user_id": "u_0001"},
        params={"latency_mean_ms": 0, "latency_std_ms": 0, "failure_rate": 1.0},
    )
    assert resp.status_code == 503


def test_failure_rate_zero_never_fails():
    client = TestClient(create_app())
    for _ in range(20):
        resp = client.post(
            "/bid",
            json={"request_id": "r1", "user_id": "u_0001"},
            params={"latency_mean_ms": 0, "latency_std_ms": 0, "failure_rate": 0.0},
        )
        assert resp.status_code == 200


def test_configurable_latency_is_actually_applied():
    client = TestClient(create_app())
    start = time.time()
    client.post(
        "/bid",
        json={"request_id": "r1", "user_id": "u_0001"},
        params={"latency_mean_ms": 50, "latency_std_ms": 0},
    )
    elapsed_ms = (time.time() - start) * 1000
    assert elapsed_ms >= 45  # allow a little slack below the 50ms mean


def test_metrics_reflect_request_and_error_counts():
    client = TestClient(create_app())
    client.post("/bid", json={"request_id": "r1", "user_id": "u_0001"}, params={"latency_mean_ms": 0, "latency_std_ms": 0})
    client.post(
        "/bid",
        json={"request_id": "r2", "user_id": "u_0002"},
        params={"latency_mean_ms": 0, "latency_std_ms": 0, "failure_rate": 1.0},
    )
    metrics = client.get("/metrics").json()
    assert metrics["request_count"] == 2
    assert metrics["error_count"] == 1
