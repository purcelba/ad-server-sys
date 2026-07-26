"""Shared fixtures for adserver/adserver's tests: real subprocesses for
feature_service and bidder_stub, the same "start the real entrypoint,
wait for /health" pattern feature_service's own AC4 test established.
"""

from __future__ import annotations

import subprocess
import sys
import time

import httpx
import pytest

from adserver.bidder_stub.service import HTTP_PORT as BIDDER_PORT
from adserver.feature_service.service import HTTP_PORT as FEATURE_SERVICE_PORT


def _wait_for_health(port: int, proc: subprocess.Popen, timeout_s: float = 30.0) -> None:
    url = f"http://localhost:{port}/health"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    proc.terminate()
    pytest.fail(f"service on :{port} did not become healthy in time")


@pytest.fixture(scope="session")
def running_feature_service():
    proc = subprocess.Popen(
        [sys.executable, "-m", "adserver.feature_service.service"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_health(FEATURE_SERVICE_PORT, proc)
    yield f"http://localhost:{FEATURE_SERVICE_PORT}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def running_bidder_stub():
    proc = subprocess.Popen(
        [sys.executable, "-m", "adserver.bidder_stub.service"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_health(BIDDER_PORT, proc)
    yield f"http://localhost:{BIDDER_PORT}"
    proc.terminate()
    proc.wait(timeout=10)
