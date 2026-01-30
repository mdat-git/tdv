# src/inspections_lakehouse/etl/etl_010_vendor_invoice_intake/run.py
from __future__ import annotations

import argparse
from pathlib import Path

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_010_vendor_invoice_intake.pipeline import run_pipeline


PIPELINE = "etl_010_vendor_invoice_intake"


def main() -> None:
    p = argparse.ArgumentParser(description="ETL010: Vendor invoice intake (attachments + Ariba email HTML)")
    p.add_argument(
        "--attachments-dir",
        default=None,
        help=(
            "Directory containing att__{messageId}__*.xlsx files. "
            "Default: INSPECTIONS_UPLOADS_ROOT/stg/invoices/attachments"
        ),
    )
    p.add_argument(
        "--email-html-dir",
        default=None,
        help=(
            "Directory containing email__{messageId}_.html files. "
            "Default: INSPECTIONS_UPLOADS_ROOT/stg/invoices/email_html"
        ),
    )
    p.add_argument("--source-system", default="ARIBA", help="Source system label for lineage")
    p.add_argument("--max-files", type=int, default=None, help="Optional: cap number of attachments processed")
    args = p.parse_args()

    run = start_run(
        PIPELINE,
        metrics={
            "attachments_dir": args.attachments_dir,
            "email_html_dir": args.email_html_dir,
            "source_system": args.source_system,
            "max_files": args.max_files,
        },
    )

    try:
        metrics = run_pipeline(
            run=run,
            attachments_dir=Path(args.attachments_dir) if args.attachments_dir else None,
            email_html_dir=Path(args.email_html_dir) if args.email_html_dir else None,
            source_system=args.source_system,
            max_files=args.max_files,
        )
        succeed_run(run, metrics=metrics)
        print("✅ ETL010 complete")
    except Exception as e:
        fail_run(run, message=str(e))
        raise


if __name__ == "__main__":
    main()
