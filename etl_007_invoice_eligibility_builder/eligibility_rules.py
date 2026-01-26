# src/inspections_lakehouse/etl/etl_007_invoice_eligibility_builder/eligibility_rules.py
from __future__ import annotations

from typing import List

import pandas as pd


def _clean_str_series(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _safe_int(s: pd.Series) -> pd.Series:
    s2 = s.astype("string").str.strip()
    s2 = s2.where(s2 != "", pd.NA)
    return pd.to_numeric(s2, errors="coerce").fillna(0).astype("int64")


def _join_reasons(row: pd.Series, cols: List[str]) -> str:
    reasons = [c for c in cols if bool(row.get(c, False))]
    return ";".join(reasons)


# ----------------------------
# Deliveries aggregation
# ----------------------------
def build_deliveries_agg(df_gcp: pd.DataFrame, *, min_images: int = 8) -> pd.DataFrame:
    """
    Aggregate deliveries evidence at vendor + scope_floc_key:
      - sum image_count across folders (handles duplicate uploads across days/folders)
      - keep helpful counts for diagnostics
    Expects columns like:
      vendor, scope_id, floc, scope_floc_key, folder, image_count (or imageCount)
    """
    df = df_gcp.copy()

    # Normalize expected columns
    if "image_count" not in df.columns and "imageCount" in df.columns:
        df["image_count"] = df["imageCount"]
    if "floc" not in df.columns and "structureId" in df.columns:
        df["floc"] = df["structureId"]

    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "folder"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    df["image_count"] = _safe_int(df["image_count"]) if "image_count" in df.columns else 0

    # Aggregate
    g = df.groupby(["vendor", "scope_floc_key"], dropna=False)
    out = g.agg(
        floc_any=("floc", "first"),
        scope_id_any=("scope_id", "first"),
        folder_count=("folder", lambda s: int(s.nunique(dropna=True))),
        image_count_total=("image_count", "sum"),
    ).reset_index()

    out["has_gcp_delivery"] = out["image_count_total"] > 0
    out["has_min_images"] = out["image_count_total"] >= int(min_images)

    return out


# ----------------------------
# Azure rollup standardization
# ----------------------------
def standardize_azure_floc(df_az_floc: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the Azure FLOC rollup table to expected columns.
    Expects at least: floc, has_any_mdoc, has_aerial_mdoc, has_ground_mdoc
    """
    df = df_az_floc.copy()
    df["floc"] = _clean_str_series(df["floc"])

    # Ensure boolean columns exist
    for c in ["has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]:
        if c not in df.columns:
            df[c] = False
        df[c] = df[c].astype("boolean").fillna(False)

    # Keep docs if present (nice to expose in output)
    keep_cols = ["floc", "has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]
    for c in [
        "aerial_measurement_documents",
        "ground_measurement_documents",
        "created_at_max",
        "created_at_min",
        "row_count",
        "object_type_count",
    ]:
        if c in df.columns:
            keep_cols.append(c)

    return df[keep_cols].drop_duplicates(subset=["floc"]).reset_index(drop=True)


# ----------------------------
# Eligibility computation
# ----------------------------
def compute_distribution_eligibility(
    *,
    scope_current: pd.DataFrame,
    deliveries_agg: pd.DataFrame,
    azure_floc: pd.DataFrame,
    min_images: int,
    run,
    source_system: str,
) -> pd.DataFrame:
    """
    Distribution buckets (per your rules):
      - DIST_DRONE: assignment_status == ACTIVE_DRONE => requires (gcp images>=min, aerial mdoc, ground mdoc)
      - DIST_HELO: assignment_status == ACTIVE_HELO => requires (gcp images>=min) only
      - DIST_COMP: scope_id == COMP => requires (ground mdoc) only (no aerial images required)
    No exceptions for now; just flags.
    Azure join is FLOC-only (unsafe).
    """
    df = scope_current.copy()
    df["asset_class"] = "distribution"

    # Normalize key cols
    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "assignment_status"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    # Join deliveries (vendor + scope_floc_key)
    df = df.merge(
        deliveries_agg,
        on=["vendor", "scope_floc_key"],
        how="left",
        suffixes=("", "_gcp"),
    )
    for c in ["image_count_total", "folder_count"]:
        if c not in df.columns:
            df[c] = 0
    df["image_count_total"] = pd.to_numeric(df["image_count_total"], errors="coerce").fillna(0).astype("int64")
    df["has_min_images"] = df.get("has_min_images", False).fillna(False).astype("boolean")
    df["has_gcp_delivery"] = df.get("has_gcp_delivery", False).fillna(False).astype("boolean")

    # Join Azure evidence by FLOC (unsafe)
    df = df.merge(azure_floc, on=["floc"], how="left", suffixes=("", "_az"))
    for c in ["has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]:
        if c not in df.columns:
            df[c] = False
        df[c] = df[c].fillna(False).astype("boolean")

    df["azure_join_method"] = "FLOC_ONLY"

    # Bucket
    df["program_bucket"] = "DIST_OTHER"
    df.loc[df["scope_id"] == "COMP", "program_bucket"] = "DIST_COMP"
    df.loc[df["assignment_status"] == "ACTIVE_HELO", "program_bucket"] = "DIST_HELO"
    df.loc[df["assignment_status"] == "ACTIVE_DRONE", "program_bucket"] = "DIST_DRONE"

    # Active requirement (base gate)
    if "is_active_assignment" in df.columns:
        df["is_active_assignment"] = df["is_active_assignment"].fillna(False).astype("boolean")
    else:
        df["is_active_assignment"] = True

    # Requirements by bucket
    df["req_images"] = False
    df["req_aerial_mdoc"] = False
    df["req_ground_mdoc"] = False

    df.loc[df["program_bucket"].isin(["DIST_DRONE", "DIST_HELO"]), "req_images"] = True
    df.loc[df["program_bucket"] == "DIST_DRONE", "req_aerial_mdoc"] = True
    df.loc[df["program_bucket"] == "DIST_DRONE", "req_ground_mdoc"] = True
    df.loc[df["program_bucket"] == "DIST_COMP", "req_ground_mdoc"] = True  # comp needs ground only

    # Evidence meets
    df["meets_images"] = (~df["req_images"]) | (df["image_count_total"] >= int(min_images))
    df["meets_aerial_mdoc"] = (~df["req_aerial_mdoc"]) | (df["has_aerial_mdoc"])
    df["meets_ground_mdoc"] = (~df["req_ground_mdoc"]) | (df["has_ground_mdoc"])

    df["meets_all_requirements"] = df["meets_images"] & df["meets_aerial_mdoc"] & df["meets_ground_mdoc"]

    df["approved_to_invoice"] = df["is_active_assignment"] & df["meets_all_requirements"]

    # Reason flags (blockers)
    df["BLOCK_INACTIVE_ASSIGNMENT"] = ~df["is_active_assignment"]
    df["BLOCK_MISSING_GCP_MIN_IMAGES"] = df["req_images"] & (df["image_count_total"] < int(min_images))
    df["BLOCK_MISSING_AERIAL_MDOC"] = df["req_aerial_mdoc"] & (~df["has_aerial_mdoc"])
    df["BLOCK_MISSING_GROUND_MDOC"] = df["req_ground_mdoc"] & (~df["has_ground_mdoc"])

    blocker_cols = [
        "BLOCK_INACTIVE_ASSIGNMENT",
        "BLOCK_MISSING_GCP_MIN_IMAGES",
        "BLOCK_MISSING_AERIAL_MDOC",
        "BLOCK_MISSING_GROUND_MDOC",
    ]
    df["block_reasons"] = df.apply(lambda r: _join_reasons(r, blocker_cols), axis=1)
    df["needs_review"] = ~df["approved_to_invoice"]

    # Lineage
    df["eligibility_run_date"] = run.run_date
    df["eligibility_run_id"] = run.run_id
    df["eligibility_source_system"] = source_system

    # Keep a clean output shape
    keep = [
        "asset_class",
        "program_bucket",
        "vendor",
        "scope_id",
        "floc",
        "scope_floc_key",
        "assignment_method",
        "assignment_status",
        "is_active_assignment",
        "image_count_total",
        "folder_count",
        "has_aerial_mdoc",
        "has_ground_mdoc",
        "has_any_mdoc",
        "azure_join_method",
        "approved_to_invoice",
        "block_reasons",
        "needs_review",
        "eligibility_run_date",
        "eligibility_run_id",
        "eligibility_source_system",
    ]
    # Keep whatever exists
    keep = [c for c in keep if c in df.columns]

    return df[keep].reset_index(drop=True)


def compute_transmission_eligibility(
    *,
    scope_current: pd.DataFrame,
    deliveries_agg: pd.DataFrame,
    min_images: int,
    run,
    source_system: str,
) -> pd.DataFrame:
    """
    Transmission rules:
      - requires aerial images only, min 8 (from deliveries evidence)
      - we still gate on is_active_assignment (safest default)
    No Azure evidence required.
    """
    df = scope_current.copy()
    df["asset_class"] = "transmission"
    df["program_bucket"] = "TRANS"

    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "assignment_status"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    # Join deliveries
    df = df.merge(deliveries_agg, on=["vendor", "scope_floc_key"], how="left", suffixes=("", "_gcp"))
    for c in ["image_count_total", "folder_count"]:
        if c not in df.columns:
            df[c] = 0
    df["image_count_total"] = pd.to_numeric(df["image_count_total"], errors="coerce").fillna(0).astype("int64")

    if "is_active_assignment" in df.columns:
        df["is_active_assignment"] = df["is_active_assignment"].fillna(False).astype("boolean")
    else:
        df["is_active_assignment"] = True

    df["req_images"] = True
    df["meets_images"] = df["image_count_total"] >= int(min_images)
    df["meets_all_requirements"] = df["meets_images"]

    df["approved_to_invoice"] = df["is_active_assignment"] & df["meets_all_requirements"]

    df["BLOCK_INACTIVE_ASSIGNMENT"] = ~df["is_active_assignment"]
    df["BLOCK_MISSING_GCP_MIN_IMAGES"] = df["image_count_total"] < int(min_images)

    blocker_cols = ["BLOCK_INACTIVE_ASSIGNMENT", "BLOCK_MISSING_GCP_MIN_IMAGES"]
    df["block_reasons"] = df.apply(lambda r: _join_reasons(r, blocker_cols), axis=1)
    df["needs_review"] = ~df["approved_to_invoice"]

    # Lineage
    df["eligibility_run_date"] = run.run_date
    df["eligibility_run_id"] = run.run_id
    df["eligibility_source_system"] = source_system

    keep = [
        "asset_class",
        "program_bucket",
        "vendor",
        "scope_id",
        "floc",
        "scope_floc_key",
        "assignment_method",
        "assignment_status",
        "is_active_assignment",
        "image_count_total",
        "folder_count",
        "approved_to_invoice",
        "block_reasons",
        "needs_review",
        "eligibility_run_date",
        "eligibility_run_id",
        "eligibility_source_system",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep].reset_index(drop=True)

# --- Add to: src/inspections_lakehouse/etl/etl_007_invoice_eligibility_builder/eligibility_rules.py

def build_unmatched_gcp(
    *,
    assignments: pd.DataFrame,
    deliveries_agg: pd.DataFrame,
    asset_class: str,
    run,
    source_system: str,
) -> pd.DataFrame:
    """
    Left anti-join:
      deliveries_agg (vendor+scope_floc_key) NOT FOUND in assignments (vendor+scope_floc_key)
    """
    a = assignments.copy()
    d = deliveries_agg.copy()

    for c in ["vendor", "scope_floc_key", "scope_id", "floc"]:
        if c in a.columns:
            a[c] = a[c].astype("string").str.strip()
        if c in d.columns:
            d[c] = d[c].astype("string").str.strip()

    a_keys = a[["vendor", "scope_floc_key"]].drop_duplicates()

    merged = d.merge(a_keys, on=["vendor", "scope_floc_key"], how="left", indicator=True)
    out = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"]).copy()

    out.insert(0, "asset_class", asset_class)

    # Helpful normalization for dashboards
    if "floc_any" in out.columns and "floc" not in out.columns:
        out = out.rename(columns={"floc_any": "floc"})
    if "scope_id_any" in out.columns and "scope_id" not in out.columns:
        out = out.rename(columns={"scope_id_any": "scope_id"})

    out["unmatched_reason"] = "VENDOR_SCOPE_KEY_NOT_IN_ASSIGNMENTS"
    out["eligibility_run_date"] = run.run_date
    out["eligibility_run_id"] = run.run_id
    out["eligibility_source_system"] = source_system

    # Optional: stable column order
    preferred = [
        "asset_class",
        "vendor",
        "scope_id",
        "floc",
        "scope_floc_key",
        "image_count_total",
        "folder_count",
        "has_gcp_delivery",
        "has_min_images",
        "unmatched_reason",
        "eligibility_run_date",
        "eligibility_run_id",
        "eligibility_source_system",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols].reset_index(drop=True)


def build_unmatched_azure(
    *,
    assignments_dist: pd.DataFrame,
    azure_floc: pd.DataFrame,
    run,
    source_system: str,
) -> pd.DataFrame:
    """
    Left anti-join:
      azure_floc (floc) NOT FOUND in distribution assignments (floc)
    (Distribution-only because Azure evidence is dist-only in your current design.)
    """
    a = assignments_dist.copy()
    z = azure_floc.copy()

    a["floc"] = a["floc"].astype("string").str.strip()
    z["floc"] = z["floc"].astype("string").str.strip()

    a_flocs = a[["floc"]].drop_duplicates()

    merged = z.merge(a_flocs, on=["floc"], how="left", indicator=True)
    out = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"]).copy()

    out.insert(0, "asset_class", "distribution")
    out["unmatched_reason"] = "FLOC_NOT_IN_DISTRIBUTION_ASSIGNMENTS"
    out["eligibility_run_date"] = run.run_date
    out["eligibility_run_id"] = run.run_id
    out["eligibility_source_system"] = source_system

    preferred = [
        "asset_class",
        "floc",
        "has_any_mdoc",
        "has_ground_mdoc",
        "has_aerial_mdoc",
        "ground_measurement_documents",
        "aerial_measurement_documents",
        "created_at_max",
        "row_count",
        "object_type_count",
        "unmatched_reason",
        "eligibility_run_date",
        "eligibility_run_id",
        "eligibility_source_system",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols].reset_index(drop=True)


def build_scope_summary(df_line: pd.DataFrame, *, run, source_system: str) -> pd.DataFrame:
    """
    Summary at vendor + scope_id (+ asset_class):
      - total lines, approved count, blocked count, approved rate
    """
    df = df_line.copy()
    for c in ["vendor", "scope_id", "asset_class"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    g = df.groupby(["asset_class", "vendor", "scope_id"], dropna=False)

    out = g.agg(
        floc_count=("scope_floc_key", "count"),
        approved_count=("approved_to_invoice", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
    ).reset_index()

    out["blocked_count"] = out["floc_count"] - out["approved_count"]
    out["approved_rate"] = (out["approved_count"] / out["floc_count"]).where(out["floc_count"] > 0, 0.0)

    out["eligibility_run_date"] = run.run_date
    out["eligibility_run_id"] = run.run_id
    out["eligibility_source_system"] = source_system

    return out
