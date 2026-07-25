import pytest
from sklearn.linear_model import LogisticRegression

from adserver.ranking.model import PctrModel


class _FakeEstimator:
    """predict_proba stand-in — avoids depending on a real fitted
    estimator for pure wrapper-behavior tests."""

    def predict_proba(self, X):
        return [[1 - 0.7, 0.7] for _ in X]


def test_predict_orders_the_feature_vector_by_pinned_names():
    seen = {}

    class _RecordingEstimator:
        def predict_proba(self, X):
            seen["X"] = X
            return [[0.9, 0.1]]

    model = PctrModel(_RecordingEstimator(), feature_names=["b", "a"])
    model.predict({"a": 1, "b": 2, "c": 999})
    assert seen["X"] == [[2, 1]]  # ordered b, a — matches feature_names(), ignores extra key "c"


def test_predict_returns_the_positive_class_probability():
    model = PctrModel(_FakeEstimator(), feature_names=["x"])
    assert model.predict({"x": 1.0}) == pytest.approx(0.7)


def test_predict_raises_clearly_on_a_missing_feature():
    model = PctrModel(_FakeEstimator(), feature_names=["a", "b"])
    with pytest.raises(KeyError, match="b"):
        model.predict({"a": 1.0})


def test_feature_names_returns_a_copy_not_the_internal_list():
    model = PctrModel(_FakeEstimator(), feature_names=["a", "b"])
    names = model.feature_names()
    names.append("c")
    assert model.feature_names() == ["a", "b"]


def test_save_and_load_round_trips_a_real_fitted_estimator(tmp_path):
    estimator = LogisticRegression().fit([[0.0], [1.0], [2.0], [3.0]], [0, 0, 1, 1])
    model = PctrModel(estimator, feature_names=["x"])

    path = tmp_path / "nested" / "model.pkl"
    model.save(path)
    assert path.exists()

    loaded = PctrModel.load(path)
    assert loaded.feature_names() == ["x"]
    assert loaded.predict({"x": 3.0}) == model.predict({"x": 3.0})


def test_load_rejects_a_pickle_that_isnt_a_pctr_model(tmp_path):
    import pickle

    path = tmp_path / "not_a_model.pkl"
    path.write_bytes(pickle.dumps({"not": "a model"}))

    with pytest.raises(TypeError):
        PctrModel.load(path)
