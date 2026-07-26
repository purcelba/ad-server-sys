import ast
import inspect

import pytest

from adserver.ranking import model_registry, scorer
from adserver.ranking.model import PctrModel
from adserver.ranking.scorer import Scorer, ScorerError, _validate_feature_names


class _FakeEstimator:
    def predict_proba(self, X):
        return [[0.4, 0.6] for _ in X]


@pytest.fixture
def registries(tmp_path):
    return {"model_registry_path": tmp_path / "models" / "registry.json", "models_dir": tmp_path / "models"}


def _register_fake_model(models_dir, model_registry_path, version, feature_names, status="live"):
    model = PctrModel(_FakeEstimator(), feature_names)
    version_dir = models_dir / "pctr" / version
    model.save(version_dir / "model.pkl")
    model_registry.register_version("pctr", version, str(version_dir), status=status, path=model_registry_path)


def test_validate_feature_names_accepts_registry_context_and_cross_names():
    feature_registry = {"user_ctr_30d": object()}
    _validate_feature_names(["user_ctr_30d", "hour_of_day", "x_user_ctr_in_ad_category"], feature_registry)


def test_validate_feature_names_rejects_an_unregistered_name():
    with pytest.raises(ScorerError, match="nonexistent_feature"):
        _validate_feature_names(["nonexistent_feature"], {})


def test_validate_feature_names_rejects_an_undefined_cross_feature():
    with pytest.raises(ScorerError, match="x_totally_made_up"):
        _validate_feature_names(["x_totally_made_up"], {})


def test_scorer_loads_the_live_version_and_scores(registries, tmp_path):
    _register_fake_model(registries["models_dir"], registries["model_registry_path"], "v1", ["hour_of_day"])

    s = Scorer(model_registry_path=registries["model_registry_path"])
    assert s.feature_names() == ["hour_of_day"]
    assert s.score({"hour_of_day": 14}) == pytest.approx(0.6)


def test_scorer_refuses_to_load_a_model_with_an_unregistered_feature(registries):
    _register_fake_model(
        registries["models_dir"], registries["model_registry_path"], "v1", ["not_a_real_feature"]
    )
    with pytest.raises(ScorerError, match="not_a_real_feature"):
        Scorer(model_registry_path=registries["model_registry_path"])


def test_scorer_with_explicit_version_bypasses_the_live_pointer(registries):
    _register_fake_model(
        registries["models_dir"], registries["model_registry_path"], "v1", ["hour_of_day"], status="live"
    )
    _register_fake_model(
        registries["models_dir"], registries["model_registry_path"], "v2", ["user_ctr_30d"], status="candidate"
    )

    live_scorer = Scorer(model_registry_path=registries["model_registry_path"])
    pinned_scorer = Scorer(model_registry_path=registries["model_registry_path"], version="v2")

    assert live_scorer.feature_names() == ["hour_of_day"]
    assert pinned_scorer.feature_names() == ["user_ctr_30d"]


def test_scorer_with_explicit_version_still_validates_feature_names(registries):
    _register_fake_model(
        registries["models_dir"], registries["model_registry_path"], "v1", ["not_a_real_feature"], status="candidate"
    )
    with pytest.raises(ScorerError, match="not_a_real_feature"):
        Scorer(model_registry_path=registries["model_registry_path"], version="v1")


def test_scorer_module_imports_no_ml_library():
    """Static AST check for AC5's opacity proof: the scorer must never
    import scikit-learn (or any ML library) directly — it only unpickles
    a PctrModel and calls its interface. AST-based (not a raw string
    search) so this can't false-positive on a docstring mentioning the
    library by name."""
    tree = ast.parse(inspect.getsource(scorer))
    forbidden = {"sklearn", "xgboost", "lightgbm", "torch", "tensorflow"}
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])
    assert not (imported_roots & forbidden), f"scorer.py imports forbidden ML library: {imported_roots & forbidden}"
