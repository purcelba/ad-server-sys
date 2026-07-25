# ranking

## What it does
A reproducible pCTR training loop with versioned, promotable model
artifacts: `train.py` builds a point-in-time training matrix from the
synthetic impression history + Phase 1's offline feature store, trains a
version from a small config (algorithm + pinned feature list +
hyperparams + seed), and writes it to `models/pctr/v{N}/`. `scorer.py` is
the library a future ad server loads — it follows whichever version
`models/registry.json` marks `live`, with zero code changes needed to
promote a new one or roll back.

**Point-in-time training data.** For each historical impression, features
must reflect what was knowable *at impression time*, not current values.
`train.py`'s `backfill()` runs Phase 1's already point-in-time-correct
`FeatureJob`s once per day across the training window (via
`batch_features.runner.run()`, called in a loop — no Phase 1 code
modified), producing one real offline Parquet partition per day; training
then reads those back via `offline_store.query_as_of()`, joined per day.
`offline_store` rows can carry `null` for a windowed feature with no data
yet in a given window (Phase 1's convention: a real "no signal", not a
fabricated zero) — `train.py`'s `_fill_registry_defaults()` applies
`feature_service`'s exact defaults policy before training, since training
reads `offline_store` directly rather than going through the service.

**Time split (fixed, not random):** the first 7 days of the 30-day
synthetic window are excluded as a feature warm-up buffer (their trailing
windows are mostly empty, and the very first day's row-count quality gate
fails outright with zero users represented — confirmed directly). Of the
remaining ~23 days, the last 5 are held out; the rest are train. Being a
fixed calendar split, plus a `--seed` threaded into the estimator's
`random_state`, is what makes `make train` exactly reproducible (AC1).

**Feature classes:** *user*/*ad* features come from `offline_store`
(batch, Phase 1); *context* is `hour_of_day`, computed inline from the
event, never stored — "slot" from the spec has no concept this project's
datagen actually produces, so it's dropped rather than invented; *cross*
is `common/crosses.py`'s `x_user_ctr_in_ad_category`, a pure function of
the other two, never stored, imported by both the training path
(`ranking/assemble.py::from_offline_row`) and the serving path
(`from_online_result`) — one implementation, so there's nothing to drift.

**Real-time features are absent from every config here — a data
limitation, not a design choice.** Redis (Phase 2) state is TTL'd and
ephemeral; there's no historical log of past Redis values to join against
a 20-day-old synthetic impression, so `V1_CONFIG`/`V2_CONFIG`'s pinned
feature lists only draw from batch + context + cross features. **The
moment a historical record of real-time feature values exists — i.e. once
Phase 5's decision log (its system of record) is logging per-request feature values (including
real-time ones) at serving time — retraining should start incorporating
them.** Concretely, that's Phase 6's `make retrain` (training from logged
decisions instead of synthetic history — Phase 5 builds the decision log
itself, Phase 6 is what actually trains from it): the natural point to add
Redis-backed features like `user_session_active` or
`user_current_ride_type` to a pinned feature list, since only then does
point-in-time training data for them exist. Nothing in `train.py` needs
to change for that — a future feature list config is all that's required.

**Model artifact contract** (`model.py`): `PctrModel` wraps any fitted
scikit-learn-API estimator (`.predict_proba`) plus its pinned, ordered
feature list, behind `predict(feature_dict) -> float` and
`feature_names() -> list[str]`. v1 (`LogisticRegression`, wrapped in a
`StandardScaler` pipeline — unscaled count features caused `lbfgs`
convergence warnings) and v2 (`HistGradientBoostingClassifier`, a
different feature list) both pickle to the identical `PctrModel` type —
nothing about the artifact's type reveals which algorithm it wraps; that
detail lives only in `training_config.json`, for humans.

**Registry + promotion** (`model_registry.py`, `promote.py`):
`models/registry.json` maps a logical name ("pctr") -> version -> `{path,
status}`. `uv run python -m adserver.ranking.promote pctr v2` flips a
version to `live` and demotes whatever was previously live to `retired`.
Rollback is the identical command with an older version.

**Scorer validation** (`scorer.py`): loads whichever version is `live`
and validates its pinned feature list — every name must be a registered
feature (`common/registry.yaml`), the one recognized context name
(`hour_of_day`), or a defined cross feature (`common/crosses.py`) —
before it will load. Imports no ML library at all; a static AST-based
test (`tests/test_scorer.py`) enforces this, which is what AC5's opacity
proof checks.

## How to run and test it alone
```bash
uv run pytest adserver/ranking adserver/common/tests/test_crosses.py -v
make train                                          # backfills + trains v1, writes models/pctr/v1/
uv run python -m adserver.ranking.train --version v2  # trains v2 too
uv run python -m adserver.ranking.promote pctr v1    # v1 -> live
uv run python -m adserver.ranking.promote pctr v2    # promote v2 (rollback: promote v1 again)
```

## Production analog
This is the local stand-in for a training pipeline + model registry — e.g.
SageMaker/Vertex AI training jobs writing to a model registry service
(MLflow, SageMaker Model Registry), with a promotion step gating what a
serving fleet actually loads. `PctrModel`'s `predict(feature_dict) ->
float` contract is the same idea as a real serving system's model-agnostic
inference interface (e.g. a Triton/TorchServe backend a ranking service
calls without knowing the framework underneath).

## Ownership note
Under an end-to-end ads team model, `ranking/` is core ads-ML territory —
not negotiable to another team the way some Phase 1-3 infrastructure
components are (e.g. the batch pipeline runner or feature service could
plausibly sit with a central ML/data platform team instead). The training
data, feature selection, and model quality bar are decisions the ads team
needs direct control over.
