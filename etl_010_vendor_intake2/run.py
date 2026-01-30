# src/inspections_lakehouse/etl/etl_010_vendor_invoices_intake/run.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Any

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_010_vendor_invoices_intake.pipeline import run_pipeline, PIPELINE


def main() -> None:
    # uploads_dir("stg") should resolve to: .../INSPECTIONS_UPLOADS/stg
    # then we want: .../INSPECTIONS_UPLOADS/stg/invoices
    default_stg_root = Path(paths.uploads_dir(category="stg", partitions={}, ensure=False)) / "invoices"

    p = argparse.ArgumentParser(description="ETL010 - Vendor invoice intake (attachments + Ariba HTML regex parsing)")
    p.add_argument(
        "--stg-root",
        type=str,
        default=str(default_stg_root),
        help="Path to .../INSPECTIONS_UPLOADS/stg/invoices (contains attachments/ and email_html/).",
    )
    p.add_argument("--source-system", type=str, default="ARIBA_EMAIL_EXPORT")
    args = p.parse_args()

    run = start_run(PIPELINE, metrics={"stg_root": args.stg_root})
    try:
        metrics: Dict[str, Any] = run_pipeline(
            run=run,
            stg_root=Path(args.stg_root),
            source_system=args.source_system,
        )
        succeed_run(run, metrics=metrics)
        print("ETL010 succeeded")
    except Exception as e:
        fail_run(run, message=str(e))
        raise


if __name__ == "__main__":
    main()
