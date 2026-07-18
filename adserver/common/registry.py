"""Feature registry: schema + loader + validation.

The registry (`registry.yaml`) is the governed contract every feature-
producing job (batch, and later streaming) must honor. A feature that
isn't declared here can't be materialized or served.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

VALID_ENTITIES = {"user", "ad"}
VALID_DTYPES = {"float", "int", "str", "list[str]", "map[str,float]"}
REQUIRED_FIELDS = [
    "name",
    "entity",
    "dtype",
    "description",
    "aggregation",
    "window",
    "freshness_sla",
    "default",
    "owner",
]

_FRESHNESS_RE = re.compile(r"^(\d+)([smhd])$")


class RegistryError(ValueError):
    """Raised on any malformed registry.yaml entry, naming the offender."""


@dataclass(frozen=True)
class FeatureDef:
    name: str
    entity: str
    dtype: str
    description: str
    aggregation: str
    window: str
    freshness_sla: str
    default: object
    owner: str

    def freshness_sla_seconds(self) -> int:
        return _parse_duration(self.freshness_sla)


def _parse_duration(value: str) -> int:
    m = _FRESHNESS_RE.match(value)
    if not m:
        raise RegistryError(
            f"invalid duration {value!r} — expected digits + unit (s|m|h|d), e.g. '24h'"
        )
    amount, unit = int(m.group(1)), m.group(2)
    multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return amount * multiplier


def _validate_entry(entry: dict) -> None:
    name = entry.get("name", "<unnamed>")
    missing = [f for f in REQUIRED_FIELDS if f not in entry]
    if missing:
        raise RegistryError(f"feature {name!r} is missing required field(s): {missing}")

    if entry["entity"] not in VALID_ENTITIES:
        raise RegistryError(
            f"feature {name!r} has invalid entity {entry['entity']!r} — must be one of {VALID_ENTITIES}"
        )
    if entry["dtype"] not in VALID_DTYPES:
        raise RegistryError(
            f"feature {name!r} has invalid dtype {entry['dtype']!r} — must be one of {VALID_DTYPES}"
        )
    _parse_duration(entry["freshness_sla"])  # raises RegistryError on bad format


def load_registry(path: Path | str) -> dict[str, FeatureDef]:
    """Load and validate registry.yaml, returning {feature_name: FeatureDef}.

    Raises RegistryError naming the offending feature/field on any
    validation failure — missing field, invalid entity/dtype, or malformed
    freshness_sla.
    """
    path = Path(path)
    raw = yaml.safe_load(path.read_text())
    entries = raw.get("features", []) if raw else []

    registry: dict[str, FeatureDef] = {}
    for entry in entries:
        _validate_entry(entry)
        name = entry["name"]
        if name in registry:
            raise RegistryError(f"duplicate feature name {name!r} in registry")
        registry[name] = FeatureDef(
            name=name,
            entity=entry["entity"],
            dtype=entry["dtype"],
            description=entry["description"],
            aggregation=entry["aggregation"],
            window=entry["window"],
            freshness_sla=entry["freshness_sla"],
            default=entry["default"],
            owner=entry["owner"],
        )
    return registry
