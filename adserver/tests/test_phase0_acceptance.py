"""Phase 0 cross-component acceptance checks (phases.md AC1).

Requires `make up` (or `docker compose up -d`) to have been run first —
this test observes infra state, it doesn't bring it up itself.
"""

import json
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _compose_ps() -> list[dict]:
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def test_infra_healthy():
    services = _compose_ps()
    if not services:
        pytest.skip("no compose services running - run `make up` first")

    names = {s["Service"]: s.get("Health") for s in services}
    expected = {"redpanda", "dynamodb-local", "redis"}
    assert expected.issubset(names.keys()), f"expected {expected}, found {set(names.keys())}"
    for service, health in names.items():
        assert health == "healthy", f"{service} is not healthy: {health}"
