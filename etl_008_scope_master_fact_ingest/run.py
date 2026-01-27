from __future__ import annotations

import argparse
from pathlib import Path

from inspections_lakehouse.util.run_logging import start_run, succeed_run, fail_run
from inspections_lakehouse.etl.etl_008_official_scope_master.pipeline import run_pipeline

PIPELINE_NAME = "etl_008_official_scope_master"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETL008 - Official scope master intake (3 XLSX: trans HF, dist HF, dist non-HF)."
    )
    p.add_argument("--trans-hf", required=True, help="Path to Transmission High-Fire scope XLSX.")
    p.add_argument("--dist-hf", required=True, help="Path to Distribution High-Fire scope XLSX.")
    p.add_argument("--dist-nhf", required=True, help="Path to Distribution Non-High-Fire scope XLSX.")

    p.add_argument(
        "--sheet",
        default="",
        help="Optional sheet name to read from all workbooks. If omitted, reads first sheet.",
    )
    p.add_argument(
        "--source-system",
        default="OFFICIAL_SCOPE_SPREADSHEET",
        help="Lineage source_system label.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    run = start_run(
        pipeline=PIPELINE_NAME,
        metrics={
            "trans_hf": str(args.trans_hf),
            "dist_hf": str(args.dist_hf),
            "dist_nhf": str(args.dist_nhf),
            "sheet": args.sheet,
            "source_system": args.source_system,
        },
    )

    try:
        metrics = run_pipeline(
            run=run,
            trans_hf=Path(args.trans_hf),
            dist_hf=Path(args.dist_hf),
            dist_nhf=Path(args.dist_nhf),
            sheet_name=(args.sheet.strip() or None),
            source_system=args.source_system,
        )
        succeed_run(run, metrics=metrics)
    except Exception as e:
        fail_run(run, message=str(e), metrics={})
        raise


if __name__ == "__main__":
    main()
