# src/inspections_lakehouse/etl/etl_006_azure_inspectapp_inspections_intake/pipeline.py
from __future__ import annotations

import shutil
import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.paths import paths, Layer
from inspections_lakehouse.etl.etl_006_azure_inspectapp_inspections_intake.silverize import (
    build_silver_line,
    build_silver_floc,
)

UPLOADS_CATEGORY = "azure_inspections"

# Distribution-only datasets (by design)
BRONZE_DATASET = "azure_inspectapp_surveys_extract_distribution"
SILVER_LINE_DATASET = "inspections_evidence_azure_distribution_line"
SILVER_FLOC_DATASET = "inspections_evidence_azure_distribution_floc"


def _pick_latest_file(stg_dir: Path) -> Path:
    if not stg_dir.exists():
        raise FileNotFoundError(f"stg_dir does not exist: {stg_dir}")

    files = sorted(stg_dir.glob("azure_inspectapp_surveys_*.csv"))
    if not files:
        raise FileNotFoundError(f"No files matching azure_inspectapp_surveys_*.csv in: {stg_dir}")

    # Prefer latest by timestamp in filename if possible, else fallback to mtime.
    # Expected (ideal): azure_inspectapp_surveys_20260126_092307Z.csv
    def sort_key(p: Path):
        m = re.search(r"_(\d{8})_(\d{6})Z\.csv$", p.name)
        if m:
            return (m.group(1), m.group(2), p.name)
        return ("00000000", "000000", p.name)

    best_by_name = sorted(files, key=sort_key)[-1]
    # If filename timestamps are inconsistent, mtime will win
    best_by_mtime = max(files, key=lambda p: p.stat().st_mtime)

    # Heuristic: if best_by_name has a real timestamp, use it; else use mtime
    return best_by_name if re.search(r"_(\d{8})_(\d{6})Z\.csv$", best_by_name.name) else best_by_mtime


def _read_csv(input_csv: Path) -> pd.DataFrame:
    """
    Read export as strings. Some exports have odd delimiters or trailing ';' columns.
    We'll detect ; vs , using header counts (cheap & effective).
    """
    sample = input_csv.read_text(encoding="utf-8", errors="ignore").splitlines()[:1]
    head = sample[0] if sample else ""
    sep = ";" if head.count(";") > head.count(",") else ","

    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False, sep=sep)
    df.columns = [c.strip().rstrip(";") for c in df.columns]
    return df


def _archive_input(input_csv: Path, run) -> str:
    up_dir = paths.uploads_dir(
        category=UPLOADS_CATEGORY,
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    dest = Path(up_dir) / input_csv.name
    shutil.copy2(input_csv, dest)
    return str(dest)


def _write_whole_table(df: pd.DataFrame, *, layer: Layer, dataset: str, run) -> Dict[str, Any]:
    out_hist = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="HISTORY",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    write_dataset(df, out_hist, basename="data")

    out_curr = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="CURRENT",
        partitions={},
        ensure=True,
    )
    write_dataset(df, out_curr, basename="data")

    return {"rows": int(len(df)), "cols": list(df.columns)}


def run_pipeline(
    *,
    stg_dir: Path,
    input_override: Optional[Path],
    run,
    source_system: str = "AZURE_INSPECTAPP",
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    input_csv = input_override if input_override else _pick_latest_file(stg_dir)
    metrics["input_csv"] = str(input_csv)

    df_raw = _read_csv(input_csv)
    metrics["raw_rows"] = int(len(df_raw))
    metrics["raw_cols"] = list(df_raw.columns)

    source_file_saved = _archive_input(input_csv, run)
    metrics["source_file_saved"] = source_file_saved

    # Bronze (raw + lineage)
    df_bronze = df_raw.copy()
    df_bronze["asset_class"] = "distribution"
    df_bronze["run_date"] = run.run_date
    df_bronze["run_id"] = run.run_id
    df_bronze["source_system"] = source_system
    df_bronze["source_file_saved"] = source_file_saved

    metrics["bronze"] = _write_whole_table(df_bronze, layer="bronze", dataset=BRONZE_DATASET, run=run)

    # Silver line + rollup
    df_line = build_silver_line(
        df_bronze=df_bronze,
        run=run,
        source_file_saved=source_file_saved,
        source_system=source_system,
    )
    metrics["silver_line_rows"] = int(len(df_line))
    metrics["silver_line"] = _write_whole_table(df_line, layer="silver", dataset=SILVER_LINE_DATASET, run=run)

    df_floc = build_silver_floc(df_line, run=run)
    metrics["silver_floc_rows"] = int(len(df_floc))
    metrics["silver_floc"] = _write_whole_table(df_floc, layer="silver", dataset=SILVER_FLOC_DATASET, run=run)

    return metrics
