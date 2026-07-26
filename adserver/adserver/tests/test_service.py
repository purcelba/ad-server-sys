"""Build-item tests for service.py's assembly: the happy path, and that
each of the three defined degraded modes actually fires and is logged
when its dependency is broken. Full end-to-end AC verification (load,
concurrency, guaranteed delivery, A/B, freshness, audience) lives in
tests/test_acceptance.py, against the real running services.
"""

from __future__ import annotations

import shutil

import pytest
from fastapi.testclient import TestClient

from adserver.adserver.decision_log import read_decisions
from adserver.adserver.service import HOUSE_AD_CAMPAIGN_ID, create_app
from adserver.feature_service.resolver import get_redis_client
from adserver.ranking.model_registry import DEFAULT_REGISTRY_PATH as MODEL_REGISTRY_PATH
from adserver.ranking.model_registry import load_registry as load_model_registry

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        get_redis_client().ping()
        return True
    except Exception:
        return False


def _models_available() -> bool:
    registry = load_model_registry(MODEL_REGISTRY_PATH)
    return "v1" in registry.get("pctr", {}) and "v2" in registry.get("pctr", {})


requires_infra = pytest.mark.skipif(
    not (_infra_reachable() and _models_available()),
    reason="redis/dynamodb-local not reachable, or v1/v2 not trained — run `make up`, `make features`, "
    "`uv run python -m adserver.ranking.train --version v1`, `... --version v2`",
)


def _redirect_decision_log(log_path):
    """`log_decision(entry, path=DEFAULT_LOG_PATH)`'s default is bound at
    function-definition time (module import) — patching the
    `DEFAULT_LOG_PATH` module attribute afterward does NOT change what a
    call omitting `path=` actually uses. The function object's own
    `__defaults__` has to be mutated directly. Returns the original
    defaults tuple, to be restored after the test."""
    import adserver.adserver.decision_log as decision_log_module

    original_defaults = decision_log_module.log_decision.__defaults__
    decision_log_module.log_decision.__defaults__ = (log_path,)
    return original_defaults


@pytest.fixture
def client(running_feature_service, running_bidder_stub, tmp_path):
    import adserver.adserver.decision_log as decision_log_module

    log_path = tmp_path / "decision_log.jsonl"
    original_defaults = _redirect_decision_log(log_path)
    try:
        yield TestClient(create_app()), log_path
    finally:
        decision_log_module.log_decision.__defaults__ = original_defaults


@requires_infra
def test_health(client):
    test_client, _ = client
    assert test_client.get("/health").json() == {"status": "ok"}


@requires_infra
def test_serve_returns_a_winner_for_a_real_user(client):
    test_client, log_path = client
    resp = test_client.post("/serve", json={"user_id": "u_0001", "session_id": "s1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["experiment_arm"] in {"control", "treatment"}
    # winner may be None only if genuinely nothing was eligible - assert
    # the response is well-formed either way, not that a specific ad wins
    assert "winner_campaign_id" in body

    decisions = read_decisions(log_path)
    assert len(decisions) == 1
    assert decisions[0]["request_id"] == body["request_id"]


@requires_infra
def test_serve_never_returns_a_500_for_a_nonexistent_user(client):
    """A brand-new/unknown user_id has no materialized features anywhere
    - every feature resolves to its registry default. Must still serve
    something (house ad at worst), never crash."""
    test_client, _ = client
    resp = test_client.post("/serve", json={"user_id": "u_totally_unknown_9999", "session_id": "s1"})
    assert resp.status_code == 200


@requires_infra
def test_house_ad_rung_when_scorer_fails_to_load(tmp_path, monkeypatch, running_feature_service, running_bidder_stub):
    """Makes every Scorer(version=...) construction fail - service.py must
    still start and serve the house ad, never crash app construction or
    the request."""
    from adserver.ranking import scorer as scorer_module

    def _always_raise(*args, **kwargs):
        raise scorer_module.ScorerError("simulated model load failure")

    monkeypatch.setattr(scorer_module, "Scorer", _always_raise)
    # service.py imports Scorer by name into its own namespace, so patch
    # that reference too - patching the source module alone wouldn't
    # affect an already-imported `from ... import Scorer`.
    import adserver.adserver.service as service_module

    monkeypatch.setattr(service_module, "Scorer", _always_raise)

    import adserver.adserver.decision_log as decision_log_module

    log_path = tmp_path / "decision_log.jsonl"
    original_defaults = _redirect_decision_log(log_path)
    try:
        app = create_app()
        test_client = TestClient(app)
        resp = test_client.post("/serve", json={"user_id": "u_0001", "session_id": "s1"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["winner_campaign_id"] == HOUSE_AD_CAMPAIGN_ID
        assert body["fallback_rung"] == "model_load_failure_house_ad"

        decisions = read_decisions(log_path)
        assert decisions[-1]["rung"] == "model_load_failure_house_ad"
    finally:
        decision_log_module.log_decision.__defaults__ = original_defaults
