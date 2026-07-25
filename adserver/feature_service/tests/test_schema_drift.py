"""Committed openapi.json is the contract snapshot for POST /features.
This test fails whenever the live app's schema diverges from it -
whether from an intentional API change (update the snapshot) or an
accidental one (fix the code)."""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from adserver.feature_service.resolver import get_redis_client
from adserver.feature_service.service import create_app

OPENAPI_SNAPSHOT_PATH = Path(__file__).parent.parent / "openapi.json"

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        get_redis_client().ping()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(not _infra_reachable(), reason="redis/dynamodb-local not reachable — run `make up`")


def _live_schema() -> dict:
    app = create_app()
    client = TestClient(app)
    return client.get("/openapi.json").json()


@requires_infra
def test_live_schema_matches_committed_snapshot():
    snapshot = json.loads(OPENAPI_SNAPSHOT_PATH.read_text())
    assert _live_schema() == snapshot, (
        "live OpenAPI schema for feature_service diverged from the committed "
        f"snapshot at {OPENAPI_SNAPSHOT_PATH} — if this change is intentional, "
        "regenerate it: curl -s localhost:8003/openapi.json | python3 -m json.tool "
        "> adserver/feature_service/openapi.json"
    )


@requires_infra
def test_schema_drift_test_actually_catches_a_change():
    """Mutation check: deliberately corrupt the snapshot and confirm the
    comparison fails, proving this test isn't vacuously true."""
    snapshot = json.loads(OPENAPI_SNAPSHOT_PATH.read_text())
    corrupted = copy.deepcopy(snapshot)
    corrupted["paths"]["/features"]["post"]["operationId"] = "deliberately_wrong"

    assert _live_schema() != corrupted
