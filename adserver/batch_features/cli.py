"""CLI entrypoint: `make features` — run all jobs, quality-gate, write
offline Parquet, and materialize to DynamoDB-local."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import typer

from adserver.batch_features.runner import DEFAULT_DATA_DIR, DEFAULT_OUTPUT_DIR, run
from adserver.datagen.users import HISTORY_END

app = typer.Typer(add_completion=False)


@app.command()
def main(
    as_of: str = typer.Option(None, help="ISO date; defaults to the synthetic history end."),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="Directory with generated parquet files."),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, help="Offline Parquet output directory."),
) -> None:
    resolved = dt.date.fromisoformat(as_of) if as_of else HISTORY_END
    combined = run(as_of=resolved, data_dir=data_dir, output_dir=output_dir, materialize_to_dynamo=True)
    for entity, df in combined.items():
        typer.echo(f"{entity}: {df.height} rows, columns: {df.columns}")
    typer.echo(f"Materialized to DynamoDB-local table 'features', offline parquet in {output_dir}/")


if __name__ == "__main__":
    app()
