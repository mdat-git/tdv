from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.validation import validate
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.vendors import assert_valid_vendor

from inspections_lakehouse.etl.etl_003_helo_scope_intake.contract import CONTRACTS
from inspections_lakehouse.etl.etl_003_helo_scope_intake.silverize import write_silver_line_and_header


PIPELINE = "etl_003_helo_scope_intake"

SHEETS_TO_DATASETS: dict[str, str] = {
    "Distribution": "helo_scope_distribution",
    "Transmission": "helo_scope_transmission",
}


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xlsx_path, sheet_name=sheet_name)


def run_pipeline(run: RunLog, *, input_path: str, vendor: str) -> None:
    assert_valid_vendor(vendor)

    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    # Save exact artifact
    upload_parts = {"vendor": vendor, **run.partitions}
    uploads_dir = paths.uploads_dir("helo_scope_intake", partitions=upload_parts, ensure=True)
    saved_src = uploads_dir / src.name
    shutil.copy2(src, saved_src)

    run.metrics.update(
        {
            "vendor": vendor,
            "source_file_original": str(src),
            "source_file_saved": str(saved_src),
        }
    )

    xl = pd.ExcelFile(saved_src)
    run.metrics["excel_sheet_names"] = xl.sheet_names

    for sheet_name, dataset_name in SHEETS_TO_DATASETS.items():
        if sheet_name not in xl.sheet_names:
            run.metrics[f"{dataset_name}__skipped"] = f"Sheet '{sheet_name}' not found"
            continue

        df = _read_sheet(saved_src, sheet_name)
        validate(df, CONTRACTS[sheet_name])

        # Bronze HISTORY + CURRENT (vendor-grain)
        hist_dir = paths.local_dir(
            "bronze",
            dataset_name,
            "HISTORY",
            partitions={"vendor": vendor, **run.partitions},
            ensure=True,
        )
        hist_file = write_dataset(df, hist_dir, basename="data")

        curr_dir = paths.local_dir(
            "bronze",
            dataset_name,
            "CURRENT",
            partitions={"vendor": vendor},
            ensure=True,
        )
        curr_file = write_dataset(df, curr_dir, basename="data")

        run.metrics.update(
            {
                f"{dataset_name}_sheet": sheet_name,
                f"{dataset_name}_rows": int(len(df)),
                f"{dataset_name}_cols": int(df.shape[1]),
                f"{dataset_name}_bronze_history_file": str(hist_file),
                f"{dataset_name}_bronze_current_file": str(curr_file),
            }
        )

        # Silver
        write_silver_line_and_header(
            df_bronze=df,
            dataset_name=dataset_name,
            sheet_name=sheet_name,
            vendor=vendor,
            run=run,
            source_file_saved=str(saved_src),
        )
