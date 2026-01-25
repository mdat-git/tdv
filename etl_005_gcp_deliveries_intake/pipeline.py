# src/inspections_lakehouse/etl/etl_005_gcp_deliveries_intake/pipeline.py
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, Optional, List

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.vendors import validate_vendor
from inspections_lakehouse.etl.etl_005_gcp_deliveries_intake.canonicalize import build_silver_evidence


UPLOADS_CATEGORY = "gcp_deliveries"


def _dataset_names(asset_class: str) -> tuple[str, str]:
    asset_class = asset_class.lower().strip()
    if asset_class not in {"distribution", "transmission"}:
        raise ValueError(f"asset_class must be 'distribution' or 'transmission', got: {asset_class}")

    bronze = f"gcp_deliveries_extract_{asset_class}"
    silver = f"deliveries_evidence_gcp_{asset_class}_line"
    return bronze, silver


def _read_csv(input_csv: Path) -> pd.DataFrame:
    return pd.read_csv(input_csv, dtype=str, keep_default_na=False)


def _archive_input(input_csv: Path, run, asset_class: str) -> str:
    up_dir = paths.uploads_dir(
        category=UPLOADS_CATEGORY,
        partitions={"asset_class": asset_class, "run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    dest = Path(up_dir) / input_csv.name
    shutil.copy2(input_csv, dest)
    return str(dest)


def _write_vendor_partitioned(df: pd.DataFrame, *, layer: str, dataset: str, run) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    vendor_values = sorted(set(df["vendor"].astype(str).tolist()))
    metrics["vendors_written"] = vendor_values
    metrics["vendor_counts"] = {}

    for v in vendor_values:
        validate_vendor(v)
        df_v = df[df["vendor"].astype(str) == v].copy()
        metrics["vendor_counts"][v] = int(len(df_v))

        out_hist = paths.local_dir(
            layer=layer,
            dataset=dataset,
            version="HISTORY",
            partitions={"vendor": v, "run_date": run.run_date, "run_id": run.run_id},
            ensure=True,
        )
        write_dataset(df_v, out_hist, basename="data")

        out_curr = paths.local_dir(
            layer=layer,
            dataset=dataset,
            version="CURRENT",
            partitions={"vendor": v},
            ensure=True,
        )
        write_dataset(df_v, out_curr, basename="data")

    return metrics


def _write_whole_table(df: pd.DataFrame, *, layer: str, dataset: str, run) -> Dict[str, Any]:
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
    input_csv: Path,
    run,
    asset_class: str,
    vendor_filter: Optional[List[str]] = None,
    source_system: str = "GCP_DELIVERIES_EXPORT",
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    bronze_dataset, silver_dataset = _dataset_names(asset_class)

    df_raw = _read_csv(input_csv)
    metrics["raw_rows"] = int(len(df_raw))
    metrics["raw_cols"] = list(df_raw.columns)
    metrics["asset_class"] = asset_class

    if vendor_filter:
        vendor_filter = [v.strip() for v in vendor_filter if v.strip()]
        df_raw = df_raw[df_raw["vendor"].astype(str).isin(vendor_filter)].copy()
        metrics["raw_rows_after_vendor_filter"] = int(len(df_raw))
        metrics["vendor_filter"] = vendor_filter

    source_file_saved = _archive_input(input_csv, run, asset_class=asset_class)
    metrics["source_file_saved"] = source_file_saved

    # Bronze (raw + minimal lineage)
    df_bronze = df_raw.copy()
    df_bronze["asset_class"] = asset_class
    df_bronze["run_date"] = run.run_date
    df_bronze["run_id"] = run.run_id
    df_bronze["source_system"] = source_system
    df_bronze["source_file_saved"] = source_file_saved

    metrics["bronze"] = _write_vendor_partitioned(df_bronze, layer="bronze", dataset=bronze_dataset, run=run)

    # Silver (canonical) — whole table, no vendor partitions
    df_silver = build_silver_evidence(
        df_bronze=df_bronze,
        run=run,
        source_file_saved=source_file_saved,
        source_system=source_system,
        asset_class=asset_class,
    )
    metrics["silver_rows"] = int(len(df_silver))
    metrics["silver"] = _write_whole_table(df_silver, layer="silver", dataset=silver_dataset, run=run)

    return metrics
