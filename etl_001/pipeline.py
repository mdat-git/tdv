from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.validation import validate
from inspections_lakehouse.util.dataset_io import write_dataset

from inspections_lakehouse.etl.etl_001_scope_release_intake.contract import CONTRACT


DATASET = "scope_release_intake"
PIPELINE = "etl_001_scope_release_intake"


def _read_input(input_path: Path) -> pd.DataFrame:
    ext = input_path.suffix.lower()
    if ext in (".csv", ".txt"):
        return pd.read_csv(input_path)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(input_path)
    raise ValueError(f"Unsupported input file extension: {ext}")


def run_pipeline(run: RunLog, *, input_path: str) -> None:
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    # 1) Save the exact source artifact (as-received)
    uploads_dir = paths.uploads_dir(
        "scope_release_intake",
        partitions=run.partitions,
        ensure=True,
    )
    saved_src = uploads_dir / src.name
    shutil.copy2(src, saved_src)

    # 2) Read
    df = _read_input(saved_src)

    # 3) Validate (start permissive, tighten later)
    validate(df, CONTRACT)

    # 4) Write BRONZE HISTORY (immutable)
    bronze_hist_dir = paths.local_dir(
        "bronze",
        DATASET,
        "HISTORY",
        partitions=run.partitions,
        ensure=True,
    )
    hist_file = write_dataset(df, bronze_hist_dir, basename="data")

    # 5) Write BRONZE CURRENT (latest pointer)
    bronze_curr_dir = paths.local_dir("bronze", DATASET, "CURRENT", ensure=True)
    curr_file = write_dataset(df, bronze_curr_dir, basename="data")

    # 6) Metrics for run log
    run.metrics.update(
        {
            "source_file_original": str(src),
            "source_file_saved": str(saved_src),
            "rows_read": int(len(df)),
            "cols_read": int(df.shape[1]),
            "bronze_history_dir": str(bronze_hist_dir),
            "bronze_history_file": str(hist_file),
            "bronze_current_dir": str(bronze_curr_dir),
            "bronze_current_file": str(curr_file),
        }
    )
