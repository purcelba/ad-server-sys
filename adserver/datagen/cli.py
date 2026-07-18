"""CLI entrypoint: generate users/campaigns/events parquet deterministically."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import typer

from adserver.datagen.campaigns import generate_campaigns
from adserver.datagen.events import generate_events
from adserver.datagen.rides import generate_rides
from adserver.datagen.users import generate_users

app = typer.Typer(add_completion=False)


def _write_parquet(df, path: Path) -> None:
    # use_pyarrow=False + no compression-level metadata quirks keeps output
    # byte-identical across runs with the same seed.
    df.write_parquet(path, use_pyarrow=False)


def run(seed: int, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    users = generate_users(rng)
    campaigns = generate_campaigns(rng)
    events = generate_events(rng, users, campaigns)
    rides = generate_rides(rng, users)

    _write_parquet(users, out / "users.parquet")
    _write_parquet(campaigns, out / "campaigns.parquet")
    _write_parquet(events, out / "events.parquet")
    _write_parquet(rides, out / "rides.parquet")


@app.command()
def main(
    seed: int = typer.Option(42, help="RNG seed; same seed -> identical output files."),
    out: Path = typer.Option(Path("data"), help="Output directory for parquet files."),
) -> None:
    run(seed, out)
    typer.echo(f"Wrote users.parquet, campaigns.parquet, events.parquet, rides.parquet to {out}/")


if __name__ == "__main__":
    app()
