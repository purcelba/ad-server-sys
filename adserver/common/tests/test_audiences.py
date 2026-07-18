from pathlib import Path

import pytest

from adserver.common.audiences import AudienceError, load_audiences

AUDIENCES_PATH = Path(__file__).resolve().parents[1] / "audiences.yaml"


def test_real_audiences_load_and_validate():
    audiences = load_audiences(AUDIENCES_PATH)
    assert set(audiences.keys()) == {"frequent_airport_travelers", "weekday_commuters"}
    assert audiences["frequent_airport_travelers"].definition_version == 1
    assert len(audiences["weekday_commuters"].rules) == 2


def _write(tmp_path, yaml_text) -> Path:
    p = tmp_path / "audiences.yaml"
    p.write_text(yaml_text)
    return p


def test_missing_required_field_raises(tmp_path):
    p = _write(
        tmp_path,
        """
audiences:
  - name: broken_audience
    rules: []
""",
    )
    with pytest.raises(AudienceError, match="broken_audience"):
        load_audiences(p)


def test_invalid_op_raises(tmp_path):
    p = _write(
        tmp_path,
        """
audiences:
  - name: bad_op_audience
    definition_version: 1
    rules:
      - feature: segment
        op: matches
        value: traveler
""",
    )
    with pytest.raises(AudienceError, match="bad_op_audience"):
        load_audiences(p)


def test_rule_missing_field_raises(tmp_path):
    p = _write(
        tmp_path,
        """
audiences:
  - name: incomplete_rule_audience
    definition_version: 1
    rules:
      - feature: segment
        op: eq
""",
    )
    with pytest.raises(AudienceError, match="incomplete_rule_audience"):
        load_audiences(p)


def test_duplicate_name_raises(tmp_path):
    p = _write(
        tmp_path,
        """
audiences:
  - name: dupe
    definition_version: 1
    rules:
      - {feature: segment, op: eq, value: traveler}
  - name: dupe
    definition_version: 2
    rules:
      - {feature: segment, op: eq, value: commuter}
""",
    )
    with pytest.raises(AudienceError, match="dupe"):
        load_audiences(p)
