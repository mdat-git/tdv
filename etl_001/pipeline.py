from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.validation import validate
from inspections_lakehouse.util.dataset_io import write_dataset

from inspections_lakehouse.etl.etl_001_scope_release_intake.contract import CONTRACTS


PIPELINE = "etl_001_scope_release_intake"

# Separate bronze datasets (recommended)
DATASETS = {
    "Distribution": "scope_release_distribution",
    "Transmission": "scope_release_transmission",
}


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xlsx_path, sheet_name=sheet_name)


def run_pipeline(run: RunLog, *, input_path: str) -> None:
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    # 1) Save exact artifact (as-received)
    uploads_dir = paths.uploads_dir("scope_release_intake", partitions=run.partitions, ensure=True)
    saved_src = uploads_dir / src.name
    shutil.copy2(src, saved_src)

    run.metrics.update(
        {
            "source_file_original": str(src),
            "source_file_saved": str(saved_src),
        }
    )

    # 2) Read both base tabs, validate, write bronze
    for sheet_name, dataset_name in DATASETS.items():
        df = _read_sheet(saved_src, sheet_name)

        # Optional: super-light normalization (safe for bronze)
        # df.columns = [c.strip() for c in df.columns]

        validate(df, CONTRACTS[sheet_name])

        # HISTORY (immutable)
        hist_dir = paths.local_dir(
            "bronze", dataset_name, "HISTORY",
            partitions=run.partitions, ensure=True
        )
        hist_file = write_dataset(df, hist_dir, basename="data")

        # CURRENT (latest pointer)
        curr_dir = paths.local_dir("bronze", dataset_name, "CURRENT", ensure=True)
        curr_file = write_dataset(df, curr_dir, basename="data")

        run.metrics.update(
            {
                f"{dataset_name}_rows": int(len(df)),
                f"{dataset_name}_cols": int(df.shape[1]),
                f"{dataset_name}_bronze_history_dir": str(hist_dir),
                f"{dataset_name}_bronze_history_file": str(hist_file),
                f"{dataset_name}_bronze_current_dir": str(curr_dir),
                f"{dataset_name}_bronze_current_file": str(curr_file),
            }
        )
