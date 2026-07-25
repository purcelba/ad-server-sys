"""Requires real Redpanda (make up) — ensure_topic() and the actual
producer.produce() call both need a live broker."""

import shutil

import pytest
from fastapi.testclient import TestClient

from adserver.ui.publish_api import create_app

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _kafka_reachable() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=5)
        return True
    except Exception:
        return False


requires_kafka = pytest.mark.skipif(not _kafka_reachable(), reason="redpanda not reachable — run `make up`")


@requires_kafka
def test_index_serves_mini_html_with_user_options():
    client = TestClient(create_app())
    resp = client.get("/")
    assert resp.status_code == 200
    assert "u_0001" in resp.text
    assert "Fire event" in resp.text


@requires_kafka
def test_health_endpoint():
    client = TestClient(create_app())
    resp = client.get("/health")
    assert resp.status_code == 200


@requires_kafka
def test_publish_event_succeeds():
    client = TestClient(create_app())
    resp = client.post(
        "/events",
        json={"user_id": "u_0001", "event_type": "destination_entered", "payload": {"category": "travel", "geo": "sea"}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["published"] is True
    assert body["event_type"] == "destination_entered"


@requires_kafka
def test_publish_event_rejects_unknown_event_type():
    client = TestClient(create_app())
    resp = client.post(
        "/events",
        json={"user_id": "u_0001", "event_type": "not_a_real_type", "payload": {}},
    )
    assert resp.status_code == 422
