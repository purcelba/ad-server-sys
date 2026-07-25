"""CLI: `uv run python -m adserver.ranking.promote <logical_name> <version>`
flips a model version to `live`. Rollback is the identical command with an
older version — there is no separate rollback CLI."""

from __future__ import annotations

import typer

from adserver.ranking.model_registry import promote as promote_version

app = typer.Typer(add_completion=False)


@app.command()
def main(logical_name: str, version: str) -> None:
    promote_version(logical_name, version)
    typer.echo(f"{logical_name} {version} is now live.")


if __name__ == "__main__":
    app()
