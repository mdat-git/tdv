# src/inspections_lakehouse/etl/etl_006_azure_inspectapp_inspections_intake/run.py
from __future__ import annotations

import argparse
from pathlib import Path

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_006_azure_inspectapp_inspections_intake.pipeline import run_pipeline

PIPELINE_NAME = "etl_006_azure_inspectapp_inspections_intake"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL006 - Azure InspectApp survey evidence intake (CSV).")
    p.add_argument(
        "--stg-dir",
        default="./INSPECTIONS_UPLOADS/stg/azure_inspections",
        help="Directory containing Power Automate CSV drops (timestamped).",
    )
    p.add_argument(
        "--input",
        default="",
        help="Optional: explicit CSV path. If omitted, ETL grabs the latest from --stg-dir.",
    )
    p.add_argument(
        "--source-system",
        default="AZURE_INSPECTAPP",
        help="Lineage field for source system.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    stg_dir = Path(args.stg_dir)
    input_path = Path(args.input) if args.input else None

    run = start_run(
        pipeline=PIPELINE_NAME,
        metrics={
            "stg_dir": str(stg_dir),
            "input_override": str(input_path) if input_path else "",
            "source_system": args.source_system,
            "asset_class": "distribution",
        },
    )

    try:
        metrics = run_pipeline(
            stg_dir=stg_dir,
            input_override=input_path,
            run=run,
            source_system=args.source_system,
        )
        succeed_run(run, metrics=metrics)
    except Exception as e:
        fail_run(run, message=str(e), metrics={"stg_dir": str(stg_dir), "input_override": str(input_path or "")})
        raise


if __name__ == "__main__":
    main()
