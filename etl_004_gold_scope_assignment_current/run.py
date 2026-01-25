from __future__ import annotations

import argparse
import sys
import traceback
from typing import List, Optional

import pandas as pd

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.util.vendors import validate_vendor
from inspections_lakehouse.etl.etl_004_gold_scope_assignment_current.pipeline import (
    PIPELINE,
    run_pipeline,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETL 004 - GOLD Scope Assignment Current (append vendors + apply events)"
    )
    p.add_argument(
        "--vendors",
        required=False,
        help="Optional comma-separated vendor list. Default = all valid vendors.",
    )
    return p.parse_args()


def _parse_vendors(vendors_arg: Optional[str]) -> Optional[List[str]]:
    if not vendors_arg:
        return None
    raw = [v.strip() for v in vendors_arg.split(",") if v.strip()]
    return [validate_vendor(v) for v in raw]


def main() -> int:
    args = parse_args()
    vendors = _parse_vendors(args.vendors)

    run = start_run(
        PIPELINE,
        metrics={
            "vendors_arg": args.vendors,
            "python_executable": sys.executable,
            "python_version": sys.version,
            "pandas_version": pd.__version__,
        },
    )

    try:
        run_pipeline(run, vendors=vendors)
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
