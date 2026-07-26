from pathlib import Path

import pytest

from adserver.ranking.model_registry import (
    ModelRegistryError,
    get_live_path,
    get_version_path,
    load_registry,
    promote,
    register_version,
)


@pytest.fixture
def registry_path(tmp_path):
    return tmp_path / "registry.json"


def test_load_registry_missing_file_returns_empty_dict(registry_path):
    assert load_registry(registry_path) == {}


def test_register_version_then_load_round_trips(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="candidate", path=registry_path)
    registry = load_registry(registry_path)
    assert registry == {"pctr": {"v1": {"path": "models/pctr/v1", "status": "candidate"}}}


def test_register_version_rejects_invalid_status(registry_path):
    with pytest.raises(ModelRegistryError):
        register_version("pctr", "v1", "models/pctr/v1", status="bogus", path=registry_path)


def test_get_live_path_raises_when_nothing_is_live(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="candidate", path=registry_path)
    with pytest.raises(ModelRegistryError, match="no live version"):
        get_live_path("pctr", registry_path)


def test_promote_flips_status_and_get_live_path_follows_it(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="candidate", path=registry_path)
    promote("pctr", "v1", registry_path)
    assert get_live_path("pctr", registry_path) == Path("models/pctr/v1")


def test_promote_demotes_the_previous_live_version_to_retired(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="live", path=registry_path)
    register_version("pctr", "v2", "models/pctr/v2", status="candidate", path=registry_path)

    promote("pctr", "v2", registry_path)

    registry = load_registry(registry_path)
    assert registry["pctr"]["v1"]["status"] == "retired"
    assert registry["pctr"]["v2"]["status"] == "live"
    assert get_live_path("pctr", registry_path) == Path("models/pctr/v2")


def test_rollback_is_just_promoting_the_older_version_again(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="live", path=registry_path)
    register_version("pctr", "v2", "models/pctr/v2", status="candidate", path=registry_path)

    promote("pctr", "v2", registry_path)
    promote("pctr", "v1", registry_path)  # rollback

    registry = load_registry(registry_path)
    assert registry["pctr"]["v1"]["status"] == "live"
    assert registry["pctr"]["v2"]["status"] == "retired"


def test_promote_unregistered_version_raises(registry_path):
    with pytest.raises(ModelRegistryError, match="v9"):
        promote("pctr", "v9", registry_path)


def test_get_version_path_returns_a_non_live_version(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="live", path=registry_path)
    register_version("pctr", "v2", "models/pctr/v2", status="candidate", path=registry_path)
    assert get_version_path("pctr", "v2", registry_path) == Path("models/pctr/v2")


def test_get_version_path_raises_for_an_unregistered_version(registry_path):
    register_version("pctr", "v1", "models/pctr/v1", status="live", path=registry_path)
    with pytest.raises(ModelRegistryError, match="v9"):
        get_version_path("pctr", "v9", registry_path)
