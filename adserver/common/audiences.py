"""Audience definitions: schema + loader + validation.

Audiences (`audiences.yaml`) are named, versioned rule sets over registry
features — a governed contract like the registry itself. A definition
change bumps `definition_version`; the version is logged every time the
audience job runs, so membership drift under an unchanged name is always
visible (see `batch_features/jobs/audiences.py`).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

VALID_OPS = {"eq", "ne", "gt", "gte", "lt", "lte"}
REQUIRED_AUDIENCE_FIELDS = ["name", "definition_version", "rules"]
REQUIRED_RULE_FIELDS = ["feature", "op", "value"]


class AudienceError(ValueError):
    """Raised on any malformed audiences.yaml entry, naming the offender."""


@dataclass(frozen=True)
class Rule:
    feature: str
    op: str
    value: object


@dataclass(frozen=True)
class AudienceDef:
    name: str
    definition_version: int
    rules: tuple[Rule, ...]


def _validate_rule(audience_name: str, rule: dict) -> None:
    missing = [f for f in REQUIRED_RULE_FIELDS if f not in rule]
    if missing:
        raise AudienceError(
            f"audience {audience_name!r} has a rule missing required field(s): {missing}"
        )
    if rule["op"] not in VALID_OPS:
        raise AudienceError(
            f"audience {audience_name!r} has a rule with invalid op {rule['op']!r} — "
            f"must be one of {VALID_OPS}"
        )


def load_audiences(path: Path | str) -> dict[str, AudienceDef]:
    """Load and validate audiences.yaml, returning {audience_name: AudienceDef}."""
    path = Path(path)
    raw = yaml.safe_load(path.read_text())
    entries = raw.get("audiences", []) if raw else []

    audiences: dict[str, AudienceDef] = {}
    for entry in entries:
        name = entry.get("name", "<unnamed>")
        missing = [f for f in REQUIRED_AUDIENCE_FIELDS if f not in entry]
        if missing:
            raise AudienceError(f"audience {name!r} is missing required field(s): {missing}")
        if name in audiences:
            raise AudienceError(f"duplicate audience name {name!r}")

        for rule in entry["rules"]:
            _validate_rule(name, rule)

        audiences[name] = AudienceDef(
            name=name,
            definition_version=entry["definition_version"],
            rules=tuple(
                Rule(feature=r["feature"], op=r["op"], value=r["value"]) for r in entry["rules"]
            ),
        )
    return audiences
