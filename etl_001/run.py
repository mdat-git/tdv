from __future__ import annotations

import argparse

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_001_scope_release_intake.pipeline import run_pipeline, PIPELINE


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL 001 - Scope release intake")
    p.add_argument("--input", required=True, help="Path to input CSV/XLSX file")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    run = start_run(PIPELINE)

    try:
        run_pipeline(run, input_path=args.input)
        succeed_run(run, metrics=run.metrics)
        print("✅ SUCCESS")
        return 0
    except Exception as e:
        fail_run(run, message=str(e), metrics=run.metrics)
        print("❌ FAILED:", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
