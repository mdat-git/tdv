# src/inspections_lakehouse/etl/etl_009_pricing_rate_card_intake/run.py
from __future__ import annotations

import argparse
from pathlib import Path

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_009_pricing_rate_card_intake.pipeline import run_pipeline


PIPELINE = "etl_009_pricing_rate_card_intake"


def main() -> None:
    p = argparse.ArgumentParser(description="ETL009: Pricing Rate Card Intake")
    p.add_argument("--input", required=True, help="Path to pricing rate card file (xlsx/csv)")
    p.add_argument("--sheet", default=None, help="Excel sheet name (optional)")
    p.add_argument("--source-system", default="FINANCE", help="Source system label for lineage")
    args = p.parse_args()

    run = start_run(PIPELINE, metrics={"input": args.input, "sheet": args.sheet, "source_system": args.source_system})

    try:
        metrics = run_pipeline(
            run=run,
            input_path=Path(args.input),
            sheet_name=args.sheet,
            source_system=args.source_system,
        )
        succeed_run(run, metrics=metrics)
        print("✅ ETL009 complete")
    except Exception as e:
        fail_run(run, message=str(e))
        raise


if __name__ == "__main__":
    main()
