from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.validation import validate
from inspections_lakehouse.util.dataset_io import write_dataset

from inspections_lakehouse.etl.etl_001_scope_release_intake.delta import compute_key_delta
from inspections_lakehouse.etl.etl_001_scope_release_intake.contract import CONTRACTS

from inspections_lakehouse.etl.etl_001_scope_release_intake.silverize import write_silver_line_and_header





PIPELINE = "etl_001_scope_release_intake"

SHEETS_TO_DATASETS: dict[str, str] = {
    "Distribution": "scope_release_distribution",
    "Transmission": "scope_release_transmission",
}


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xlsx_path, sheet_name=sheet_name)


def run_pipeline(run: RunLog, *, input_path: str, vendor: str, release: str | None) -> None:
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    # --- Save exact artifact (as received) ---
    upload_parts: dict[str, str] = {"vendor": vendor, **run.partitions}

    uploads_dir = paths.uploads_dir("scope_release_intake", partitions=upload_parts, ensure=True)
    saved_src = uploads_dir / src.name
    shutil.copy2(src, saved_src)

    # Log basic provenance
    run.metrics.update(
        {
            "vendor": vendor,
            "release": release,
            "source_file_original": str(src),
            "source_file_saved": str(saved_src),
        }
    )

    # Log discovered sheets (helps debugging)
    xl = pd.ExcelFile(saved_src)
    run.metrics["excel_sheet_names"] = xl.sheet_names

    # --- For each sheet: read -> validate -> write bronze history/current (vendor-grain) ---
    for sheet_name, dataset_name in SHEETS_TO_DATASETS.items():
        df = _read_sheet(saved_src, sheet_name)

        # Optional very-light cleanup safe for bronze:
        # df.columns = [str(c).strip() for c in df.columns]

        validate(df, CONTRACTS[sheet_name])

        # 1) HISTORY: immutable snapshot for this vendor + run
        hist_parts: dict[str, str] = {"vendor": vendor, **run.partitions}
        hist_dir = paths.local_dir("bronze", dataset_name, "HISTORY", partitions=hist_parts, ensure=True)
        hist_file = write_dataset(df, hist_dir, basename="data")

        # 2) DELTA: compare this run's HISTORY vs previous HISTORY (same vendor + dataset)
        delta_metrics, _ = compute_key_delta(
            dataset=dataset_name,
            vendor=vendor,
            run_partitions=run.partitions,
        )
        run.metrics.update({f"{dataset_name}__{k}": v for k, v in delta_metrics.items()})

        write_silver_line_and_header(
            df_bronze=df,
            dataset_name=dataset_name,
            sheet_name=sheet_name,
            vendor=vendor,
            run=run,
            source_file_saved=str(saved_src),
            release=release,
        )
        
        # 3) CURRENT: latest snapshot for this vendor (pointer-ish)
        curr_dir = paths.local_dir("bronze", dataset_name, "CURRENT", partitions={"vendor": vendor}, ensure=True)
        curr_file = write_dataset(df, curr_dir, basename="data")

        # Metrics
        run.metrics.update(
            {
                f"{dataset_name}_sheet": sheet_name,
                f"{dataset_name}_rows": int(len(df)),
                f"{dataset_name}_cols": int(df.shape[1]),
                f"{dataset_name}_bronze_history_dir": str(hist_dir),
                f"{dataset_name}_bronze_history_file": str(hist_file),
                f"{dataset_name}_bronze_current_dir": str(curr_dir),
                f"{dataset_name}_bronze_current_file": str(curr_file),
            }
        )

        
