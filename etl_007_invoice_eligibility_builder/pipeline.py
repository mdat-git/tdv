# src/inspections_lakehouse/etl/etl_007_invoice_eligibility_builder/pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from inspections_lakehouse.util.paths import paths, Layer
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.etl.etl_007_invoice_eligibility_builder.eligibility_rules import (
    build_deliveries_agg,
    standardize_azure_floc,
    compute_distribution_eligibility,
    compute_transmission_eligibility,
    build_scope_summary,
    build_unmatched_gcp,
    build_unmatched_azure,
)


PIPELINE = "etl_007_invoice_eligibility_builder"

# Inputs (per your confirmed names)
GOLD_SCOPE_DIST = "scope_assignment_current_distribution"
GOLD_SCOPE_TRANS = "scope_assignment_current_transmission"

SILVER_GCP_DIST = "deliveries_evidence_gcp_distribution_line"
SILVER_GCP_TRANS = "deliveries_evidence_gcp_transmission_line"

SILVER_AZ_DIST_FLOC = "inspections_evidence_azure_distribution_floc"

GOLD_UNMATCHED_GCP_DIST = "evidence_unmatched_gcp_distribution_line"
GOLD_UNMATCHED_GCP_TRANS = "evidence_unmatched_gcp_transmission_line"
GOLD_UNMATCHED_AZ_DIST = "evidence_unmatched_azure_distribution_floc"


# Outputs
GOLD_OUT_LINE = "invoice_eligibility_line"
GOLD_OUT_SUMMARY = "invoice_eligibility_scope_summary"


def _read_dataset_dir(dataset_dir: Path) -> pd.DataFrame:
    """
    Reads the dataset written by write_dataset(): prefer data.parquet else data.csv.
    """
    pq = dataset_dir / "data.parquet"
    csv = dataset_dir / "data.csv"
    if pq.exists():
        return pd.read_parquet(pq)
    if csv.exists():
        return pd.read_csv(csv, dtype=str, keep_default_na=False)
    raise FileNotFoundError(f"Could not find data.parquet or data.csv in: {dataset_dir}")


def _read_current(layer: Layer, dataset: str) -> pd.DataFrame:
    d = paths.local_dir(layer=layer, dataset=dataset, version="CURRENT", partitions={}, ensure=False)
    return _read_dataset_dir(Path(d))


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


def run_pipeline(*, run, min_images: int = 8, source_system: str = "INTERNAL") -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # ----------------------------
    # Load Inputs
    # ----------------------------
    df_scope_dist = _read_current("gold", GOLD_SCOPE_DIST)
    df_scope_trans = _read_current("gold", GOLD_SCOPE_TRANS)

    df_gcp_dist = _read_current("silver", SILVER_GCP_DIST)
    df_gcp_trans = _read_current("silver", SILVER_GCP_TRANS)

    df_az_floc = _read_current("silver", SILVER_AZ_DIST_FLOC)

    metrics["inputs"] = {
        "scope_dist_rows": int(len(df_scope_dist)),
        "scope_trans_rows": int(len(df_scope_trans)),
        "gcp_dist_rows": int(len(df_gcp_dist)),
        "gcp_trans_rows": int(len(df_gcp_trans)),
        "az_floc_rows": int(len(df_az_floc)),
    }

    # ----------------------------
    # Prep evidence
    # ----------------------------
    deliveries_dist = build_deliveries_agg(df_gcp_dist, min_images=min_images)
    deliveries_trans = build_deliveries_agg(df_gcp_trans, min_images=min_images)

    az_floc = standardize_azure_floc(df_az_floc)

    metrics["evidence"] = {
        "deliveries_dist_agg_rows": int(len(deliveries_dist)),
        "deliveries_trans_agg_rows": int(len(deliveries_trans)),
        "az_floc_std_rows": int(len(az_floc)),
    }

    # ----------------------------
    # Compute eligibility (dist/trans separately)
    # ----------------------------
    out_dist = compute_distribution_eligibility(
        scope_current=df_scope_dist,
        deliveries_agg=deliveries_dist,
        azure_floc=az_floc,
        min_images=min_images,
        run=run,
        source_system=source_system,
    )
    out_trans = compute_transmission_eligibility(
        scope_current=df_scope_trans,
        deliveries_agg=deliveries_trans,
        min_images=min_images,
        run=run,
        source_system=source_system,
    )

    # Unified line table
    out_line = pd.concat([out_dist, out_trans], ignore_index=True, sort=False)
    metrics["out_line_rows"] = int(len(out_line))

    # Scope summary (vendor + scope_id)
    out_summary = build_scope_summary(out_line, run=run, source_system=source_system)
    metrics["out_summary_rows"] = int(len(out_summary))

    # ----------------------------
    # Write outputs
    # ----------------------------
    metrics["write_line"] = _write_whole_table(out_line, layer="gold", dataset=GOLD_OUT_LINE, run=run)
    metrics["write_summary"] = _write_whole_table(out_summary, layer="gold", dataset=GOLD_OUT_SUMMARY, run=run)

    return metrics
