"""Reach report: member counts and pairwise overlap per audience — the
"how many riders would this campaign reach?" question a sales team asks
before selling a targeted campaign.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import typer

from adserver.batch_features.framework import DEFAULT_DATA_DIR
from adserver.batch_features.jobs.audiences import AudienceMembershipsJob

app = typer.Typer(add_completion=False)


def compute_reach(as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> dict:
    df = AudienceMembershipsJob().compute(as_of, data_dir)
    members: dict[str, set[str]] = {}
    for row in df.iter_rows(named=True):
        for name in row["audience_memberships"]:
            members.setdefault(name, set()).add(row["user_id"])

    counts = {name: len(users) for name, users in members.items()}
    overlaps = {}
    names = sorted(members.keys())
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            overlaps[(a, b)] = len(members[a] & members[b])

    return {"counts": counts, "overlaps": overlaps, "total_users": df.height}


def print_reach(as_of: dt.date, data_dir: Path = DEFAULT_DATA_DIR) -> None:
    report = compute_reach(as_of, data_dir)
    typer.echo(f"Reach report as of {as_of.isoformat()} ({report['total_users']} users total)\n")
    typer.echo("Member counts:")
    for name, count in sorted(report["counts"].items()):
        typer.echo(f"  {name}: {count}")

    if report["overlaps"]:
        typer.echo("\nPairwise overlap:")
        for (a, b), n in sorted(report["overlaps"].items()):
            typer.echo(f"  {a} ∩ {b}: {n}")


@app.command()
def main(
    as_of: str = typer.Option(None, help="ISO date; defaults to today's synthetic history end."),
    data_dir: Path = typer.Option(Path("data"), help="Directory with generated parquet files."),
) -> None:
    if as_of is None:
        from adserver.datagen.users import HISTORY_END

        resolved = HISTORY_END
    else:
        resolved = dt.date.fromisoformat(as_of)
    print_reach(resolved, data_dir)


if __name__ == "__main__":
    app()
