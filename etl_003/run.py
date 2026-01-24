from __future__ import annotations

import argparse

from inspections_lakehouse.util.run_logging import start_run
from inspections_lakehouse.etl.etl_003_helo_scope_intake.pipeline import run_pipeline, PIPELINE


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog=PIPELINE)
    p.add_argument("--input", required=True, help="Path to HELO scope XLSX")
    p.add_argument("--vendor", required=True, help="HELO vendor name (validated in vendors.py)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    with start_run(PIPELINE) as run:
        run_pipeline(run, input_path=args.input, vendor=args.vendor)


if __name__ == "__main__":
    main()
