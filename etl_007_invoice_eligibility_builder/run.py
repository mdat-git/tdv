# src/inspections_lakehouse/etl/etl_007_invoice_eligibility_builder/run.py
from __future__ import annotations

import argparse

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_007_invoice_eligibility_builder.pipeline import run_pipeline

PIPELINE_NAME = "etl_007_invoice_eligibility_builder"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL007 - Invoice Eligibility Builder (GOLD).")
    p.add_argument(
        "--min-images",
        type=int,
        default=8,
        help="Minimum image count for aerial deliveries evidence (default: 8).",
    )
    p.add_argument(
        "--source-system",
        default="INTERNAL",
        help="Lineage marker for this ETL output.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    run = start_run(
        pipeline=PIPELINE_NAME,
        metrics={
            "min_images": args.min_images,
            "source_system": args.source_system,
        },
    )

    try:
        metrics = run_pipeline(run=run, min_images=args.min_images, source_system=args.source_system)
        succeed_run(run, metrics=metrics)
    except Exception as e:
        fail_run(run, message=str(e), metrics={"min_images": args.min_images})
        raise


if __name__ == "__main__":
    main()
