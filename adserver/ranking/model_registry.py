"""Minimal model registry: `models/registry.json` maps a logical model
name (e.g. "pctr") -> version -> {path, status}. `status` is one of
`candidate` | `live` | `retired`. Exactly one version per logical name may
be `live` at a time — the scorer follows that pointer, never a hardcoded
path.

Deliberately a separate module/file from `common/registry.py`, which is
the *feature* registry (a different governed contract entirely).
"""

from __future__ import annotations

import json
from pathlib import Path

DEFAULT_REGISTRY_PATH = Path("models/registry.json")
VALID_STATUSES = {"candidate", "live", "retired"}


class ModelRegistryError(ValueError):
    pass


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> dict:
    path = Path(path)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_registry(registry: dict, path: Path = DEFAULT_REGISTRY_PATH) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n")


def register_version(
    logical_name: str,
    version: str,
    artifact_path: str,
    status: str = "candidate",
    path: Path = DEFAULT_REGISTRY_PATH,
) -> None:
    """Adds/overwrites one version's entry. Never touches `live` status of
    any other version — a freshly trained candidate must be explicitly
    promoted, never silently becomes live."""
    if status not in VALID_STATUSES:
        raise ModelRegistryError(f"status {status!r} must be one of {VALID_STATUSES}")
    registry = load_registry(path)
    registry.setdefault(logical_name, {})
    registry[logical_name][version] = {"path": artifact_path, "status": status}
    save_registry(registry, path)


def promote(logical_name: str, version: str, path: Path = DEFAULT_REGISTRY_PATH) -> None:
    """Flips `version` to `live`, demoting whatever was previously live
    (if any) to `retired`. Rollback is just calling this again with an
    older version — no separate rollback code path needed."""
    registry = load_registry(path)
    if logical_name not in registry or version not in registry[logical_name]:
        raise ModelRegistryError(f"{logical_name!r} version {version!r} is not registered")

    for entry in registry[logical_name].values():
        if entry["status"] == "live":
            entry["status"] = "retired"
    registry[logical_name][version]["status"] = "live"
    save_registry(registry, path)


def get_live_path(logical_name: str, path: Path = DEFAULT_REGISTRY_PATH) -> Path:
    registry = load_registry(path)
    entries = registry.get(logical_name, {})
    live_versions = [v for v, entry in entries.items() if entry["status"] == "live"]

    if not live_versions:
        raise ModelRegistryError(f"no live version registered for {logical_name!r}")
    if len(live_versions) > 1:
        raise ModelRegistryError(f"multiple live versions for {logical_name!r}: {live_versions}")

    return Path(entries[live_versions[0]]["path"])
