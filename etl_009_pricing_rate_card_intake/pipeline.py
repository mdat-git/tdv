# src/inspections_lakehouse/etl/etl_009_pricing_rate_card_intake/pipeline.py
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from inspections_lakehouse.util.paths import paths, Layer
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.etl.etl_009_pricing_rate_card_intake.silverize import silverize_rate_card


PIPELINE = "etl_009_pricing_rate_card_intake"

BRONZE_DATASET = "pricing_rate_card_raw"
SILVER_DATASET = "pricing_rate_card_line"


def _read_input(path: Path, *, sheet_name: str | None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(str(path))

    suf = path.suffix.lower()
    if suf in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    if suf == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def _write_whole_table(df: pd.DataFrame, *, layer: Layer, dataset: str, run) -> Dict[str, Any]:
    out_hist = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="HISTORY",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    write_dataset(df, Path(out_hist), basename="data")

    out_curr = paths.local_dir(layer=layer, dataset=dataset, version="CURRENT", partitions={}, ensure=True)
    write_dataset(df, Path(out_curr), basename="data")

    return {"rows": int(len(df)), "cols": list(df.columns)}


def _archive_input_file(input_path: Path, *, run) -> Path:
    """
    Save the raw input file for provenance under INSPECTIONS_UPLOADS_ROOT/pricing_rate_card/...
    """
    out_dir = paths.uploads_dir(
        category="pricing_rate_card",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    out_path = out_dir / input_path.name
    shutil.copy2(input_path, out_path)
    return out_path


def run_pipeline(*, run, input_path: Path, sheet_name: str | None, source_system: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # ----------------------------
    # Read input
    # ----------------------------
    df_raw = _read_input(input_path, sheet_name=sheet_name)
    metrics["read"] = {"rows": int(len(df_raw)), "cols": list(df_raw.columns)}

    # ----------------------------
    # Archive raw file (provenance)
    # ----------------------------
    archived_path = _archive_input_file(input_path, run=run)
    metrics["archive"] = {"saved_to": str(archived_path)}

    # ----------------------------
    # Bronze write (raw snapshot)
    # ----------------------------
    metrics["write_bronze"] = _write_whole_table(df_raw, layer="bronze", dataset=BRONZE_DATASET, run=run)

    # ----------------------------
    # Silverize
    # ----------------------------
    df_silver = silverize_rate_card(df_raw, run=run, source_system=source_system)
    metrics["silverize"] = {"rows": int(len(df_silver)), "cols": list(df_silver.columns)}

    # ----------------------------
    # Silver write (normalized)
    # ----------------------------
    metrics["write_silver"] = _write_whole_table(df_silver, layer="silver", dataset=SILVER_DATASET, run=run)

    return metrics
