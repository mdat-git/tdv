# src/inspections_lakehouse/etl/etl_007_invoice_eligibility_builder/eligibility_rules.py
from __future__ import annotations

from typing import List

import pandas as pd


# ----------------------------
# Small helpers
# ----------------------------
def _clean_str_series(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _safe_int(s: pd.Series) -> pd.Series:
    """
    Convert a series to int64 safely:
      - treats "" as NA
      - coerces non-numeric to NA
      - fills NA with 0
    """
    s2 = s.astype("string").str.strip()
    s2 = s2.where(s2 != "", pd.NA)
    return pd.to_numeric(s2, errors="coerce").fillna(0).astype("int64")


def _join_reasons(row: pd.Series, cols: List[str]) -> str:
    """
    NA-safe reason joiner:
      - ONLY counts literal True as a reason
      - avoids bool(pd.NA) ambiguity
    """
    reasons: List[str] = []
    for c in cols:
        v = row.get(c, False)
        if v is True:
            reasons.append(c)
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

    # Clean key text columns if present
    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "folder"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    # Ensure image_count exists
    if "image_count" not in df.columns:
        df["image_count"] = 0
    df["image_count"] = _safe_int(df["image_count"])

    # Group/aggregate
    g = df.groupby(["vendor", "scope_floc_key"], dropna=False)
    out = g.agg(
        floc_any=("floc", "first"),
        scope_id_any=("scope_id", "first"),
        folder_count=("folder", lambda s: int(pd.Series(s).nunique(dropna=True))),
        image_count_total=("image_count", "sum"),
    ).reset_index()

    out["has_gcp_delivery"] = (out["image_count_total"].fillna(0).astype("int64") > 0).astype("boolean")
    out["has_min_images"] = (out["image_count_total"].fillna(0).astype("int64") >= int(min_images)).astype("boolean")

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

    if "floc" not in df.columns:
        raise ValueError("Azure FLOC rollup must include column: floc")

    df["floc"] = _clean_str_series(df["floc"])

    # Ensure boolean columns exist and are clean
    for c in ["has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]:
        if c not in df.columns:
            df[c] = False
        df[c] = df[c].fillna(False).astype("boolean")

    keep_cols = ["floc", "has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]

    # Optional diagnostic columns (keep if present)
    optional = [
        "aerial_measurement_documents",
        "ground_measurement_documents",
        "created_at_max",
        "created_at_min",
        "row_count",
        "object_type_count",
    ]
    keep_cols.extend([c for c in optional if c in df.columns])

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
      - DIST_HELO:  assignment_status == ACTIVE_HELO  => requires (gcp images>=min) only
      - DIST_COMP:  scope_id == COMP => requires (ground mdoc) only (no aerial images required)

    Notes:
      - Azure join is FLOC-only (unsafe).
      - EZ_POLE on distribution is NEVER billable.
    """
    df = scope_current.copy()
    df["asset_class"] = "distribution"

    # Normalize key cols
    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "assignment_status", "assignment_method"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    # ----------------------------
    # Join deliveries (vendor + scope_floc_key)
    # ----------------------------
    df = df.merge(
        deliveries_agg,
        on=["vendor", "scope_floc_key"],
        how="left",
        suffixes=("", "_gcp"),
    )

    if "image_count_total" not in df.columns:
        df["image_count_total"] = 0
    if "folder_count" not in df.columns:
        df["folder_count"] = 0

    df["image_count_total"] = pd.to_numeric(df["image_count_total"], errors="coerce").fillna(0).astype("int64")
    df["folder_count"] = pd.to_numeric(df["folder_count"], errors="coerce").fillna(0).astype("int64")

    if "has_min_images" not in df.columns:
        df["has_min_images"] = False
    else:
        df["has_min_images"] = df["has_min_images"].fillna(False).astype("boolean")

    if "has_gcp_delivery" not in df.columns:
        df["has_gcp_delivery"] = False
    else:
        df["has_gcp_delivery"] = df["has_gcp_delivery"].fillna(False).astype("boolean")

    # ----------------------------
    # Join Azure evidence by FLOC (unsafe)
    # ----------------------------
    df = df.merge(azure_floc, on=["floc"], how="left", suffixes=("", "_az"))

    for c in ["has_any_mdoc", "has_aerial_mdoc", "has_ground_mdoc"]:
        if c not in df.columns:
            df[c] = False
        df[c] = df[c].fillna(False).astype("boolean")

    df["azure_join_method"] = "FLOC_ONLY"

    # ----------------------------
    # Program bucket (IMPORTANT precedence: COMP overrides)
    # ----------------------------
    df["program_bucket"] = "DIST_OTHER"
    df.loc[df.get("assignment_status", pd.Series("", index=df.index)).eq("ACTIVE_HELO"), "program_bucket"] = "DIST_HELO"
    df.loc[df.get("assignment_status", pd.Series("", index=df.index)).eq("ACTIVE_DRONE"), "program_bucket"] = "DIST_DRONE"
    df.loc[df.get("scope_id", pd.Series("", index=df.index)).eq("COMP"), "program_bucket"] = "DIST_COMP"  # override last

    # ----------------------------
    # Active requirement (base gate)
    # ----------------------------
    if "is_active_assignment" in df.columns:
        df["is_active_assignment"] = df["is_active_assignment"].fillna(False).astype("boolean")
    else:
        df["is_active_assignment"] = pd.Series(True, index=df.index, dtype="boolean")

    # ----------------------------
    # Requirements by bucket
    # ----------------------------
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

    # ----------------------------
    # Program acronym + billing bucket + billable flag
    # ----------------------------
    PROGRAM_MAP = {
        "DIST_DRONE": "D360",
        "DIST_COMP": "ODI",
        "DIST_HELO": "HELO",
        "DIST_OTHER": "DIST_OTHER",
    }

    df["program_acronym"] = df["program_bucket"].map(PROGRAM_MAP).fillna(df["program_bucket"])

    # Default billing bucket = operational program (most rows)
    df["billing_bucket"] = df["program_acronym"]

    # Work completion (evidence only)
    df["is_work_complete"] = df["meets_all_requirements"].fillna(False).astype("boolean")

    # Billable is rule/policy-based (NOT evidence-based)
    df["is_billable"] = (df["is_active_assignment"] & (df["program_acronym"] != "DIST_OTHER")).fillna(False).astype("boolean")

    # EZ_POLE special rule (distribution never billable)
    if "object_type" in df.columns:
        obj = df["object_type"].astype("string")
    else:
        obj = pd.Series(pd.NA, index=df.index, dtype="string")

    ez_mask = obj.fillna("").str.strip().eq("EZ_POLE")

    df.loc[ez_mask, "billing_bucket"] = "NON_BILLABLE"
    df.loc[ez_mask, "is_billable"] = False

    # Final approval = work complete + billable
    df["approved_to_invoice"] = (df["is_work_complete"] & df["is_billable"]).fillna(False).astype("boolean")

    # ----------------------------
    # Reason flags (blockers)
    # ----------------------------
    df["BLOCK_INACTIVE_ASSIGNMENT"] = (~df["is_active_assignment"]).fillna(False).astype("boolean")
    df["BLOCK_MISSING_GCP_MIN_IMAGES"] = (df["req_images"] & (df["image_count_total"] < int(min_images))).fillna(False).astype("boolean")
    df["BLOCK_MISSING_AERIAL_MDOC"] = (df["req_aerial_mdoc"] & (~df["has_aerial_mdoc"])).fillna(False).astype("boolean")
    df["BLOCK_MISSING_GROUND_MDOC"] = (df["req_ground_mdoc"] & (~df["has_ground_mdoc"])).fillna(False).astype("boolean")
    df["BLOCK_EZ_POLE_DIST_NOT_BILLABLE"] = ez_mask.fillna(False).astype("boolean")

    blocker_cols = [
        "BLOCK_INACTIVE_ASSIGNMENT",
        "BLOCK_MISSING_GCP_MIN_IMAGES",
        "BLOCK_MISSING_AERIAL_MDOC",
        "BLOCK_MISSING_GROUND_MDOC",
        "BLOCK_EZ_POLE_DIST_NOT_BILLABLE",
    ]

    df["block_reasons"] = df.apply(lambda r: _join_reasons(r, blocker_cols), axis=1)
    df["needs_review"] = (~df["approved_to_invoice"]).fillna(True).astype("boolean")

    # ----------------------------
    # Lineage
    # ----------------------------
    df["eligibility_run_date"] = run.run_date
    df["eligibility_run_id"] = run.run_id
    df["eligibility_source_system"] = source_system

    # Keep a clean output shape (only keep if exists)
    keep = [
        "asset_class",
        "program_bucket",
        "program_acronym",
        "billing_bucket",
        "is_work_complete",
        "is_billable",
        "vendor",
        "scope_id",
        "floc",
        "scope_floc_key",
        "object_type",
        "voltage",
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
      - requires aerial images only, min_images (from deliveries evidence)
      - gates on is_active_assignment (safest default)
    No Azure evidence required.
    """
    df = scope_current.copy()
    df["asset_class"] = "transmission"
    df["program_bucket"] = "TRANS"

    for c in ["vendor", "scope_id", "floc", "scope_floc_key", "assignment_status", "assignment_method"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    # Join deliveries
    df = df.merge(deliveries_agg, on=["vendor", "scope_floc_key"], how="left", suffixes=("", "_gcp"))

    if "image_count_total" not in df.columns:
        df["image_count_total"] = 0
    if "folder_count" not in df.columns:
        df["folder_count"] = 0

    df["image_count_total"] = pd.to_numeric(df["image_count_total"], errors="coerce").fillna(0).astype("int64")
    df["folder_count"] = pd.to_numeric(df["folder_count"], errors="coerce").fillna(0).astype("int64")

    if "is_active_assignment" in df.columns:
        df["is_active_assignment"] = df["is_active_assignment"].fillna(False).astype("boolean")
    else:
        df["is_active_assignment"] = pd.Series(True, index=df.index, dtype="boolean")

    df["req_images"] = True
    df["meets_images"] = (df["image_count_total"] >= int(min_images)).fillna(False).astype("boolean")
    df["meets_all_requirements"] = df["meets_images"].fillna(False).astype("boolean")

    # Program acronym + billing bucket
    df["program_acronym"] = "ATI"
    df["billing_bucket"] = "ATI"

    # Work completion is evidence-only
    df["is_work_complete"] = df["meets_all_requirements"].fillna(False).astype("boolean")

    # Billable is policy-based (active gate)
    df["is_billable"] = df["is_active_assignment"].fillna(False).astype("boolean")

    # Final approval
    df["approved_to_invoice"] = (df["is_work_complete"] & df["is_billable"]).fillna(False).astype("boolean")

    # Blockers
    df["BLOCK_INACTIVE_ASSIGNMENT"] = (~df["is_active_assignment"]).fillna(False).astype("boolean")
    df["BLOCK_MISSING_GCP_MIN_IMAGES"] = (df["image_count_total"] < int(min_images)).fillna(False).astype("boolean")

    blocker_cols = ["BLOCK_INACTIVE_ASSIGNMENT", "BLOCK_MISSING_GCP_MIN_IMAGES"]
    df["block_reasons"] = df.apply(lambda r: _join_reasons(r, blocker_cols), axis=1)
    df["needs_review"] = (~df["approved_to_invoice"]).fillna(True).astype("boolean")

    # Lineage
    df["eligibility_run_date"] = run.run_date
    df["eligibility_run_id"] = run.run_id
    df["eligibility_source_system"] = source_system

    keep = [
        "asset_class",
        "program_bucket",
        "program_acronym",
        "billing_bucket",
        "is_work_complete",
        "is_billable",
        "vendor",
        "scope_id",
        "floc",
        "scope_floc_key",
        "object_type",
        "voltage",
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


# ----------------------------
# Forensics outputs
# ----------------------------
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
    """
    a = assignments_dist.copy()
    z = azure_floc.copy()

    if "floc" not in a.columns:
        raise ValueError("assignments_dist must include floc")
    if "floc" not in z.columns:
        raise ValueError("azure_floc must include floc")

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
    Summary at asset_class + vendor + scope_id:
      - floc_count, approved_count, blocked_count, approved_rate
    """
    df = df_line.copy()

    for c in ["vendor", "scope_id", "asset_class"]:
        if c in df.columns:
            df[c] = _clean_str_series(df[c])

    if "approved_to_invoice" not in df.columns:
        df["approved_to_invoice"] = False

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

    return out.reset_index(drop=True)
