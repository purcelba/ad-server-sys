"""The scorer: loaded by the (future) ad server, always follows whichever
model version `model_registry.json` currently marks `live`. Imports no ML
library at all — it only unpickles a `PctrModel` and calls its
`predict()`/`feature_names()`, which is what keeps it opaque to which
algorithm a version actually uses (AC5's opacity proof checks this
statically: no sklearn import anywhere in this module's source).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH as DEFAULT_FEATURE_REGISTRY_PATH
from adserver.common.crosses import CROSS_FUNCTIONS
from adserver.common.registry import load_registry
from adserver.ranking import model_registry
from adserver.ranking.model import PctrModel

LOGICAL_NAME = "pctr"

# The one non-registry, non-cross feature name a pinned feature list is
# allowed to reference — "computed inline from the event, never stored"
# per the phase-4 feature-class contract. Extend this set if a future
# model config adds another context feature.
CONTEXT_FEATURE_NAMES = {"hour_of_day"}


class ScorerError(ValueError):
    pass


def _validate_feature_names(feature_names: list[str], feature_registry: dict) -> None:
    """Every pinned name must be one of: a registered user/ad feature, the
    one recognized context feature, or a defined cross feature (`x_`
    prefix). Anything else means a model was trained against a feature
    that no longer exists (or never did) — refuse to load rather than
    silently score with an undefined signal."""
    for name in feature_names:
        if name.startswith("x_"):
            if name not in CROSS_FUNCTIONS:
                raise ScorerError(f"{name!r} is a cross feature but is not defined in common/crosses.py")
        elif name in CONTEXT_FEATURE_NAMES:
            continue
        elif name not in feature_registry:
            raise ScorerError(
                f"{name!r} is not a registered feature, a known context feature, or a defined cross feature"
            )


class Scorer:
    def __init__(
        self,
        logical_name: str = LOGICAL_NAME,
        model_registry_path: Path = model_registry.DEFAULT_REGISTRY_PATH,
        feature_registry_path: Path = DEFAULT_FEATURE_REGISTRY_PATH,
        version: str | None = None,
    ):
        """`version`, if given, bypasses the `live` pointer entirely and
        loads that specific version instead — Phase 5's A/B assignment
        needs two specific versions loaded simultaneously (e.g. arm
        control -> v1, arm treatment -> v2), independent of whichever one
        is currently live. Additive, backward-compatible change to
        already-tagged phase-4 code (default None preserves the original
        "whichever is live" behavior); flagged per CLAUDE.md's standing
        instruction."""
        if version is not None:
            resolved_path = Path(model_registry.get_version_path(logical_name, version, model_registry_path))
        else:
            resolved_path = Path(model_registry.get_live_path(logical_name, model_registry_path))
        feature_registry = load_registry(feature_registry_path)

        model = PctrModel.load(resolved_path / "model.pkl")
        _validate_feature_names(model.feature_names(), feature_registry)

        self._model = model
        self.version_path = resolved_path

    def score(self, feature_dict: dict[str, Any]) -> float:
        return self._model.predict(feature_dict)

    def feature_names(self) -> list[str]:
        return self._model.feature_names()
