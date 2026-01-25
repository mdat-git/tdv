# src/inspections_lakehouse/etl/etl_005_gcp_deliveries_intake/run.py
from __future__ import annotations

import argparse
from pathlib import Path

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_005_gcp_deliveries_intake.pipeline import run_pipeline

PIPELINE_NAME = "etl_005_gcp_deliveries_intake"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL005 - GCP deliveries CSV intake (evidence).")
    p.add_argument("--input", required=True, help="Path to GCP deliveries CSV extract.")
    p.add_argument(
        "--asset-class",
        required=True,
        choices=["distribution", "transmission"],
        help="Which asset class this file represents.",
    )
    p.add_argument("--vendors", default="", help="Optional comma-separated vendor filter.")
    p.add_argument("--source-system", default="GCP_DELIVERIES_EXPORT", help="Lineage field for source system.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    vendor_filter = [v.strip() for v in args.vendors.split(",") if v.strip()] if args.vendors else None
    input_path = Path(args.input)

    run = start_run(
        pipeline=PIPELINE_NAME,
        metrics={
            "input_path": str(input_path),
            "asset_class": args.asset_class,
            "vendor_filter": vendor_filter or "ALL",
            "source_system": args.source_system,
        },
    )

    try:
        metrics = run_pipeline(
            input_csv=input_path,
            run=run,
            asset_class=args.asset_class,
            vendor_filter=vendor_filter,
            source_system=args.source_system,
        )
        succeed_run(run, metrics=metrics)
    except Exception as e:
        fail_run(run, message=str(e), metrics={"input_path": str(input_path), "asset_class": args.asset_class})
        raise


if __name__ == "__main__":
    main()
