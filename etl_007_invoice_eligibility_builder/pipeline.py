from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional

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

# Inputs
GOLD_SCOPE_DIST = "scope_assignment_current_distribution"
GOLD_SCOPE_TRANS = "scope_assignment_current_transmission"

SILVER_GCP_DIST = "deliveries_evidence_gcp_distribution_line"
SILVER_GCP_TRANS = "deliveries_evidence_gcp_transmission_line"

SILVER_AZ_DIST_FLOC = "inspections_evidence_azure_distribution_floc"

# NEW: authoritative dim from ETL008
SILVER_FLOC_ATTR_DIM = "floc_attributes_dim"

# Forensics outputs
GOLD_UNMATCHED_GCP_DIST = "evidence_unmatched_gcp_distribution_line"
GOLD_UNMATCHED_GCP_TRANS = "evidence_unmatched_gcp_transmission_line"
GOLD_UNMATCHED_AZ_DIST = "evidence_unmatched_azure_distribution_floc"

# Outputs
GOLD_OUT_LINE = "invoice_eligibility_line"
GOLD_OUT_SUMMARY = "invoice_eligibility_scope_summary"


def _read_dataset_dir(dataset_dir: Path) -> pd.DataFrame:
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


def _standardize_floc_attributes_dim(df_dim: pd.DataFrame) -> pd.DataFrame:
    """
    Expect at least: floc, object_type (voltage optional).
    Keep one row per floc deterministically.
    """
    df = df_dim.copy()

    if "floc" not in df.columns:
        raise ValueError("floc_attributes_dim missing required column: floc")
    if "object_type" not in df.columns:
        raise ValueError("floc_attributes_dim missing required column: object_type")

    df["floc"] = df["floc"].astype("string").str.strip()
    df["object_type"] = df["object_type"].astype("string").str.strip()
    if "voltage" in df.columns:
        df["voltage"] = df["voltage"].astype("string").str.strip()
        df.loc[df["voltage"] == "", "voltage"] = pd.NA
    else:
        df["voltage"] = pd.NA

    # rank object types so duplicates resolve deterministically
    def rank_obj(x: Optional[str]) -> int:
        if x == "EZ_POLE":
            return 4
        if x == "ET_TOWER":
            return 3
        if x == "ET_POLE":
            return 2
        if x == "ED_POLE":
            return 1
        return 0

    df["_rank"] = df["object_type"].map(rank_obj)
    df = df.sort_values(["floc", "_rank"], ascending=[True, False])
    df = df.drop_duplicates(subset=["floc"], keep="first").drop(columns=["_rank"])

    return df[["floc", "object_type", "voltage"]]


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

    # NEW: floc attributes dim
    df_floc_dim = _read_current("silver", SILVER_FLOC_ATTR_DIM)
    df_floc_dim = _standardize_floc_attributes_dim(df_floc_dim)

    metrics["inputs"] = {
        "scope_dist_rows": int(len(df_scope_dist)),
        "scope_trans_rows": int(len(df_scope_trans)),
        "gcp_dist_rows": int(len(df_gcp_dist)),
        "gcp_trans_rows": int(len(df_gcp_trans)),
        "az_floc_rows": int(len(df_az_floc)),
        "floc_attr_dim_rows": int(len(df_floc_dim)),
    }

    # ----------------------------
    # Merge floc attributes onto scope assignments (no logic changes yet)
    # ----------------------------
    if "floc" in df_scope_dist.columns:
        df_scope_dist["floc"] = df_scope_dist["floc"].astype("string").str.strip()
    if "floc" in df_scope_trans.columns:
        df_scope_trans["floc"] = df_scope_trans["floc"].astype("string").str.strip()

    df_scope_dist = df_scope_dist.merge(df_floc_dim, on="floc", how="left")
    df_scope_trans = df_scope_trans.merge(df_floc_dim, on="floc", how="left")

    metrics["floc_attr_merge"] = {
        "dist_object_type_nulls": int(df_scope_dist["object_type"].isna().sum()) if "object_type" in df_scope_dist.columns else None,
        "trans_object_type_nulls": int(df_scope_trans["object_type"].isna().sum()) if "object_type" in df_scope_trans.columns else None,
        "dist_voltage_nulls": int(df_scope_dist["voltage"].isna().sum()) if "voltage" in df_scope_dist.columns else None,
        "trans_voltage_nulls": int(df_scope_trans["voltage"].isna().sum()) if "voltage" in df_scope_trans.columns else None,
        "note": "Joined floc_attributes_dim onto scope assignments (floc). No eligibility logic change yet.",
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

    # ----------------------------
    # Forensics: evidence that DID NOT merge to assignments
    # ----------------------------
    unmatched_gcp_dist = build_unmatched_gcp(
        assignments=df_scope_dist,
        deliveries_agg=deliveries_dist,
        asset_class="distribution",
        run=run,
        source_system=source_system,
    )
    unmatched_gcp_trans = build_unmatched_gcp(
        assignments=df_scope_trans,
        deliveries_agg=deliveries_trans,
        asset_class="transmission",
        run=run,
        source_system=source_system,
    )
    unmatched_az_dist = build_unmatched_azure(
        assignments_dist=df_scope_dist,
        azure_floc=az_floc,
        run=run,
        source_system=source_system,
    )

    metrics["unmatched"] = {
        "unmatched_gcp_dist_rows": int(len(unmatched_gcp_dist)),
        "unmatched_gcp_trans_rows": int(len(unmatched_gcp_trans)),
        "unmatched_az_dist_rows": int(len(unmatched_az_dist)),
    }

    # Unified line table
    out_line = pd.concat([out_dist, out_trans], ignore_index=True, sort=False)
    metrics["out_line_rows"] = int(len(out_line))

    # Scope summary (vendor + scope_id)
    out_summary = build_scope_summary(out_line, run=run, source_system=source_system)
    metrics["out_summary_rows"] = int(len(out_summary))

    # Write unmatched outputs
    metrics["write_unmatched_gcp_dist"] = _write_whole_table(
        unmatched_gcp_dist, layer="gold", dataset=GOLD_UNMATCHED_GCP_DIST, run=run
    )
    metrics["write_unmatched_gcp_trans"] = _write_whole_table(
        unmatched_gcp_trans, layer="gold", dataset=GOLD_UNMATCHED_GCP_TRANS, run=run
    )
    metrics["write_unmatched_az_dist"] = _write_whole_table(
        unmatched_az_dist, layer="gold", dataset=GOLD_UNMATCHED_AZ_DIST, run=run
    )

    # Write main outputs
    metrics["write_line"] = _write_whole_table(out_line, layer="gold", dataset=GOLD_OUT_LINE, run=run)
    metrics["write_summary"] = _write_whole_table(out_summary, layer="gold", dataset=GOLD_OUT_SUMMARY, run=run)

    return metrics
