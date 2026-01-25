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


BRONZE_DATASET = "gcp_deliveries_extract"
SILVER_DATASET = "deliveries_evidence_gcp_line"
UPLOADS_CATEGORY = "gcp_deliveries"


def _read_csv(input_csv: Path) -> pd.DataFrame:
    # Keep as raw as possible in Bronze, but ensure we read reliably.
    # Your dates are like "01-15-2026" -> parse later in canonicalize.
    return pd.read_csv(input_csv, dtype=str, keep_default_na=False)


def _archive_input(input_csv: Path, run) -> str:
    """
    Copy the exact input file to uploads for provenance.
    We partition uploads by run_date/run_id (not vendor) because the file can contain multiple vendors.
    """
    up_dir = paths.uploads_dir(
        category=UPLOADS_CATEGORY,
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    dest = Path(up_dir) / input_csv.name
    shutil.copy2(input_csv, dest)
    return str(dest)


def _write_vendor_partitioned(
    df: pd.DataFrame,
    *,
    layer: str,
    dataset: str,
    run,
    vendor_col: str = "vendor",
) -> Dict[str, Any]:
    """
    Write HISTORY + CURRENT for each vendor present in df.
    """
    metrics: Dict[str, Any] = {}
    vendor_values = sorted(set(df[vendor_col].astype(str).tolist()))

    metrics["vendors_written"] = vendor_values
    metrics["vendor_counts"] = {}

    for v in vendor_values:
        validate_vendor(v)
        df_v = df[df[vendor_col].astype(str) == v].copy()
        metrics["vendor_counts"][v] = int(len(df_v))

        # HISTORY
        out_hist = paths.local_dir(
            layer=layer,
            dataset=dataset,
            version="HISTORY",
            partitions={"vendor": v, "run_date": run.run_date, "run_id": run.run_id},
            ensure=True,
        )
        write_dataset(df_v, out_hist, basename="data")

        # CURRENT
        out_curr = paths.local_dir(
            layer=layer,
            dataset=dataset,
            version="CURRENT",
            partitions={"vendor": v},
            ensure=True,
        )
        write_dataset(df_v, out_curr, basename="data")

    return metrics


def run_pipeline(
    *,
    input_csv: Path,
    run,
    vendor_filter: Optional[List[str]] = None,
    source_system: str = "GCP_DELIVERIES_EXPORT",
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # 1) Read raw CSV
    df_raw = _read_csv(input_csv)
    metrics["raw_rows"] = int(len(df_raw))
    metrics["raw_cols"] = list(df_raw.columns)

    # Optional vendor filter at raw stage (still archive full input for provenance)
    if vendor_filter:
        vendor_filter = [v.strip() for v in vendor_filter if v.strip()]
        df_raw = df_raw[df_raw["vendor"].astype(str).isin(vendor_filter)].copy()
        metrics["raw_rows_after_vendor_filter"] = int(len(df_raw))
        metrics["vendor_filter"] = vendor_filter

    # 2) Uploads archive
    source_file_saved = _archive_input(input_csv, run)
    metrics["source_file_saved"] = source_file_saved

    # 3) Bronze write (as-is, but add minimal lineage)
    df_bronze = df_raw.copy()
    df_bronze["run_date"] = run.run_date
    df_bronze["run_id"] = run.run_id
    df_bronze["source_system"] = source_system
    df_bronze["source_file_saved"] = source_file_saved

    bronze_metrics = _write_vendor_partitioned(
        df_bronze,
        layer="bronze",
        dataset=BRONZE_DATASET,
        run=run,
        vendor_col="vendor",
    )
    metrics["bronze"] = bronze_metrics

    # 4) Silver canonicalize
    df_silver = build_silver_evidence(
        df_bronze=df_bronze,
        run=run,
        source_file_saved=source_file_saved,
        source_system=source_system,
    )
    metrics["silver_rows"] = int(len(df_silver))

    silver_metrics = _write_vendor_partitioned(
        df_silver,
        layer="silver",
        dataset=SILVER_DATASET,
        run=run,
        vendor_col="vendor",
    )
    metrics["silver"] = silver_metrics

    return metrics
