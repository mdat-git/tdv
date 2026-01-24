from __future__ import annotations

import argparse
import sys
import traceback

import pandas as pd

from inspections_lakehouse.util.vendors import validate_vendor
from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_003_helo_scope_intake.pipeline import run_pipeline, PIPELINE


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL 003 - HELO scope intake (vendor-grain)")
    p.add_argument("--input", required=True, help="Path to HELO tracker XLSX file")
    p.add_argument("--vendor", required=True, help="Vendor name (must be in VALID_VENDORS)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    vendor = validate_vendor(args.vendor)

    # Start run (writes RUNNING run.json immediately)
    run = start_run(
        PIPELINE,
        metrics={
            "vendor": vendor,
            "python_executable": sys.executable,
            "python_version": sys.version,
            "pandas_version": pd.__version__,
        },
    )

    try:
        run_pipeline(run, input_path=args.input, vendor=vendor)
        succeed_run(run, metrics=run.metrics)
        print("✅ SUCCESS")
        return 0

    except Exception as e:
        tb = traceback.format_exc()
        fail_run(run, message=str(e), metrics={**run.metrics, "traceback": tb})
        print("❌ FAILED:", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
