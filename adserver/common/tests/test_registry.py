from pathlib import Path

import pytest

from adserver.common.registry import RegistryError, load_registry

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "registry.yaml"


def test_real_registry_loads_and_validates():
    registry = load_registry(REGISTRY_PATH)
    assert len(registry) == 9
    assert "user_ctr_by_category_30d" in registry
    assert registry["user_ctr_by_category_30d"].entity == "user"
    assert registry["ad_ctr_7d"].entity == "ad"


def test_freshness_sla_parses_to_seconds():
    registry = load_registry(REGISTRY_PATH)
    assert registry["user_rides_per_week"].freshness_sla_seconds() == 24 * 3600


def _write(tmp_path, yaml_text) -> Path:
    p = tmp_path / "registry.yaml"
    p.write_text(yaml_text)
    return p


def test_missing_required_field_raises(tmp_path):
    p = _write(
        tmp_path,
        """
features:
  - name: broken_feature
    entity: user
    dtype: float
    description: missing several fields
""",
    )
    with pytest.raises(RegistryError, match="broken_feature"):
        load_registry(p)


def test_invalid_entity_raises(tmp_path):
    p = _write(
        tmp_path,
        """
features:
  - name: bad_entity_feature
    entity: campaign
    dtype: float
    description: entity should be user or ad
    aggregation: ctr
    window: 7d
    freshness_sla: 24h
    default: 0.0
    owner: ads-ml
""",
    )
    with pytest.raises(RegistryError, match="bad_entity_feature"):
        load_registry(p)


def test_invalid_dtype_raises(tmp_path):
    p = _write(
        tmp_path,
        """
features:
  - name: bad_dtype_feature
    entity: user
    dtype: bool
    description: bool is not a supported dtype
    aggregation: ctr
    window: 7d
    freshness_sla: 24h
    default: false
    owner: ads-ml
""",
    )
    with pytest.raises(RegistryError, match="bad_dtype_feature"):
        load_registry(p)


def test_invalid_freshness_sla_raises(tmp_path):
    p = _write(
        tmp_path,
        """
features:
  - name: bad_sla_feature
    entity: user
    dtype: float
    description: freshness_sla should be digits+unit
    aggregation: ctr
    window: 7d
    freshness_sla: tomorrow
    default: 0.0
    owner: ads-ml
""",
    )
    with pytest.raises(RegistryError):
        load_registry(p)


def test_duplicate_feature_name_raises(tmp_path):
    p = _write(
        tmp_path,
        """
features:
  - name: dupe
    entity: user
    dtype: float
    description: first
    aggregation: ctr
    window: 7d
    freshness_sla: 24h
    default: 0.0
    owner: ads-ml
  - name: dupe
    entity: user
    dtype: float
    description: second
    aggregation: ctr
    window: 7d
    freshness_sla: 24h
    default: 0.0
    owner: ads-ml
""",
    )
    with pytest.raises(RegistryError, match="dupe"):
        load_registry(p)
