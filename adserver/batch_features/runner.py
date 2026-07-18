"""Pipeline runner: auto-discovers FeatureJob subclasses in
batch_features/jobs/, computes each, validates against the registry, and
writes date-partitioned offline Parquet output.

Auto-discovery (rather than a hand-maintained job list) is what makes the
Phase 1 extensibility proof possible: a new feature family is one new
module in jobs/ plus registry entries — this file never needs editing.
"""

from __future__ import annotations

import datetime as dt
import importlib
import pkgutil
from pathlib import Path

import polars as pl

from adserver.batch_features import jobs as jobs_pkg
from adserver.batch_features import quality
from adserver.batch_features.framework import DEFAULT_DATA_DIR, FeatureJob
from adserver.common.registry import FeatureDef, load_registry

DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parents[1] / "common" / "registry.yaml"
DEFAULT_OUTPUT_DIR = Path("data/features")


class RunnerError(ValueError):
    pass


def discover_jobs() -> list[FeatureJob]:
    jobs: list[FeatureJob] = []
    for _, module_name, _ in pkgutil.iter_modules(jobs_pkg.__path__):
        if module_name.startswith("_"):
            continue
        module = importlib.import_module(f"{jobs_pkg.__name__}.{module_name}")
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, FeatureJob)
                and attr is not FeatureJob
                and attr.__module__ == module.__name__
            ):
                jobs.append(attr())
    return jobs


def _validate_output(job: FeatureJob, df: pl.DataFrame, registry: dict[str, FeatureDef]) -> None:
    id_col = job.entity_id_column()
    expected_cols = {id_col, *job.outputs()}
    actual_cols = set(df.columns)
    if actual_cols != expected_cols:
        raise RunnerError(
            f"{type(job).__name__}.compute() returned columns {sorted(actual_cols)}, "
            f"expected {sorted(expected_cols)} (id column {id_col!r} + outputs {job.outputs()})"
        )
    for name in job.outputs():
        if name not in registry:
            raise RunnerError(
                f"{type(job).__name__} produces {name!r}, which is not declared in registry.yaml"
            )
        if registry[name].entity != job.entity():
            raise RunnerError(
                f"{type(job).__name__} declares entity {job.entity()!r} for {name!r}, "
                f"but registry says {registry[name].entity!r}"
            )


def run(
    as_of: dt.date,
    data_dir: Path = DEFAULT_DATA_DIR,
    registry_path: Path = DEFAULT_REGISTRY_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    jobs: list[FeatureJob] | None = None,
) -> dict[str, pl.DataFrame]:
    """Run all discovered jobs, validate, and write offline Parquet output.

    `jobs` defaults to auto-discovery; pass explicitly only for testing
    (e.g. injecting a job that should fail the quality gate).

    Returns {entity: combined_frame} for the caller (materialization,
    quality gate) to use without re-reading from disk.
    """
    registry = load_registry(registry_path)
    all_jobs = jobs if jobs is not None else discover_jobs()

    by_entity: dict[str, list[pl.DataFrame]] = {"user": [], "ad": []}
    id_col_by_entity = {"user": "user_id", "ad": "campaign_id"}

    for job in all_jobs:
        df = job.compute(as_of, data_dir)
        _validate_output(job, df, registry)
        expected_count = job.expected_entity_count(as_of, data_dir)
        quality.check(df, expected_count, job.outputs(), type(job).__name__)
        by_entity[job.entity()].append(df)

    combined: dict[str, pl.DataFrame] = {}
    for entity, frames in by_entity.items():
        if not frames:
            continue
        id_col = id_col_by_entity[entity]
        merged = frames[0]
        for frame in frames[1:]:
            merged = merged.join(frame, on=id_col, how="full", coalesce=True)
        combined[entity] = merged

        partition_dir = output_dir / f"entity={entity}" / f"asof={as_of.isoformat()}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        merged.write_parquet(partition_dir / "features.parquet", use_pyarrow=False)

    return combined
