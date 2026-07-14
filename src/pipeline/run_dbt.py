"""Helpers for running dbt transformations."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt"
PROJECT_ROOT = DBT_PROJECT_DIR.parent


def run_dbt(command: str, *, target: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    if target:
        env["DBT_TARGET"] = target

    args = [
        "dbt",
        command,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
    ]
    if target:
        args.extend(["--target", target])
    if command == "run":
        args.append("--full-refresh")

    result = subprocess.run(
        args,
        env=env,
        cwd=str(PROJECT_ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt {command} failed (exit {result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def run_transforms(*, target: str | None = None) -> None:
    run_dbt("clean", target=target)
    run_dbt("run", target=target)
    run_dbt("test", target=target)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run dbt models and tests.")
    parser.add_argument("--target", default=os.getenv("DBT_TARGET", "dev"))
    args = parser.parse_args()
    run_transforms(target=args.target)
    print("dbt run and test completed successfully")


if __name__ == "__main__":
    main()
