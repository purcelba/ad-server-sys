"""Model artifact contract: every trained version is loadable as a
`PctrModel`, exposing only `predict(feature_dict) -> float` and
`feature_names() -> list[str]`. The scorer speaks only this interface —
it never imports scikit-learn (or any ML library) directly, and never
even sees which estimator a version wraps. A model version is opaque:
the algorithm and hyperparameters are internal details recorded in the
artifact directory's `training_config.json` for humans, not part of the
artifact's identity or type.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

MODEL_FILENAME = "model.pkl"


class PctrModel:
    """Wraps any fitted estimator exposing `.predict_proba(X)` (scikit-
    learn's standard classifier interface) plus the pinned, ordered
    feature name list it was trained on. Pickled directly — this class
    itself, not the raw estimator, is what gets saved and loaded, so v1
    and v2 unpickle to the identical type regardless of algorithm."""

    def __init__(self, estimator: Any, feature_names: list[str]):
        self._estimator = estimator
        self._feature_names = list(feature_names)

    def feature_names(self) -> list[str]:
        return list(self._feature_names)

    def predict(self, feature_dict: dict[str, Any]) -> float:
        missing = [name for name in self._feature_names if name not in feature_dict]
        if missing:
            raise KeyError(f"feature_dict is missing required feature(s): {missing}")
        vector = [[feature_dict[name] for name in self._feature_names]]
        return float(self._estimator.predict_proba(vector)[0][1])

    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(path: Path) -> "PctrModel":
        with open(path, "rb") as f:
            model = pickle.load(f)
        if not isinstance(model, PctrModel):
            raise TypeError(f"{path} does not contain a PctrModel artifact (got {type(model)!r})")
        return model
