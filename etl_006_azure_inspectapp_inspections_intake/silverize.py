# src/inspections_lakehouse/etl/etl_006_azure_inspectapp_inspections_intake/silverize.py
from __future__ import annotations

import re
from typing import Iterable

import pandas as pd


# Column prefix in the export: remove it everywhere
EXPORT_PREFIX_RE = re.compile(r"^stg_azure_inspectapp_inspections_all_", re.IGNORECASE)

REQUIRED_COLS = [
    "flocNumber",
    "createdAt",
    "aerialInspectionKind",
    "aerialMeasurementDocument",
    "inspectionKind",
    "measurementDocument",
    "inspectionStatus",
    "objectType",
    "workOrderNumber",
]


NULL_TOKENS = {"", "null", "none", "nan", "na", "n/a"}


def _strip_export_prefix_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [EXPORT_PREFIX_RE.sub("", c.strip().rstrip(";")) for c in df.columns]
    return df


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")


def _clean_nullish(s: pd.Series) -> pd.Series:
    s2 = s.astype(str).str.strip()
    s2 = s2.where(~s2.str.lower().isin(NULL_TOKENS), pd.NA)
    return s2


def _to_bool_true_false(s: pd.Series) -> pd.Series:
    # Export uses TRUE/FALSE (strings)
    s2 = _clean_nullish(s).astype("string")
    return s2.str.upper().map({"TRUE": True, "FALSE": False}).astype("boolean")


def _join_unique(values: Iterable[str]) -> str | None:
    uniq = []
    seen = set()
    for v in values:
        if v is None:
            continue
        v2 = str(v).strip()
        if not v2 or v2.lower() in NULL_TOKENS:
            continue
        if v2 not in seen:
            seen.add(v2)
            uniq.append(v2)
    return ";".join(uniq) if uniq else None


def build_silver_line(
    *,
    df_bronze: pd.DataFrame,
    run,
    source_file_saved: str,
    source_system: str,
) -> pd.DataFrame:
    """
    Record-level canonicalization (distribution-only by design).
    Note: no scope_id/vendor available; key is FLOC evidence.
    """
    df = _strip_export_prefix_columns(df_bronze)
    _require_columns(df, REQUIRED_COLS)

    out = pd.DataFrame()

    out["asset_class"] = "distribution"
    out["floc"] = _clean_nullish(df["flocNumber"]).astype("string")

    # createdAt like "1/13/2026 3:24:51PM"
    out["created_at"] = pd.to_datetime(_clean_nullish(df["createdAt"]), errors="coerce")

    out["aerial_inspection_kind"] = _clean_nullish(df["aerialInspectionKind"]).astype("string")
    out["aerial_measurement_document"] = _clean_nullish(df["aerialMeasurementDocument"]).astype("string")

    out["ground_inspection_kind"] = _clean_nullish(df["inspectionKind"]).astype("string")
    out["ground_measurement_document"] = _clean_nullish(df["measurementDocument"]).astype("string")

    out["inspection_status"] = _clean_nullish(df["inspectionStatus"]).astype("string")
    out["object_type"] = _clean_nullish(df["objectType"]).astype("string")
    out["work_order_number"] = _clean_nullish(df["workOrderNumber"]).astype("string")

    # Optional flags if present
    if "readyToSync" in df.columns:
        out["ready_to_sync"] = _to_bool_true_false(df["readyToSync"])
    if "syncedToSAP" in df.columns:
        out["synced_to_sap"] = _to_bool_true_false(df["syncedToSAP"])
    if "shouldSyncToSAP" in df.columns:
        out["should_sync_to_sap"] = _to_bool_true_false(df["shouldSyncToSAP"])
    if "syncStatus" in df.columns:
        out["sync_status"] = _clean_nullish(df["syncStatus"]).astype("string")

    # Evidence booleans
    out["has_ground_mdoc"] = out["ground_measurement_document"].notna()
    out["has_aerial_mdoc"] = out["aerial_measurement_document"].notna()
    out["has_any_mdoc"] = out["has_ground_mdoc"] | out["has_aerial_mdoc"]

    # Lineage
    out["source_system"] = source_system
    out["run_date"] = run.run_date
    out["run_id"] = run.run_id
    out["source_file_saved"] = source_file_saved

    # Drop rows with no FLOC
    out = out[out["floc"].notna()].reset_index(drop=True)
    return out


def build_silver_floc(df_line: pd.DataFrame, *, run) -> pd.DataFrame:
    """
    FLOC-level rollup (safe for merging evidence):
      - join-unique document IDs with ';' to preserve all
      - booleans indicate existence
      - created_at_max helps “latest activity”
    """
    g = df_line.groupby(["asset_class", "floc"], dropna=False)

    out = g.agg(
        created_at_min=("created_at", "min"),
        created_at_max=("created_at", "max"),
        object_type_count=("object_type", lambda s: int(s.nunique(dropna=True))),
        row_count=("floc", "size"),
        has_any_mdoc=("has_any_mdoc", "max"),
        has_ground_mdoc=("has_ground_mdoc", "max"),
        has_aerial_mdoc=("has_aerial_mdoc", "max"),
    ).reset_index()

    joined = g.agg(
        object_types=("object_type", lambda s: _join_unique(s.dropna().tolist())),
        inspection_statuses=("inspection_status", lambda s: _join_unique(s.dropna().tolist())),
        aerial_inspection_kinds=("aerial_inspection_kind", lambda s: _join_unique(s.dropna().tolist())),
        ground_inspection_kinds=("ground_inspection_kind", lambda s: _join_unique(s.dropna().tolist())),
        aerial_measurement_documents=("aerial_measurement_document", lambda s: _join_unique(s.dropna().tolist())),
        ground_measurement_documents=("ground_measurement_document", lambda s: _join_unique(s.dropna().tolist())),
        work_order_numbers=("work_order_number", lambda s: _join_unique(s.dropna().tolist())),
    ).reset_index()

    df_out = out.merge(joined, on=["asset_class", "floc"], how="left")
    df_out["run_date"] = run.run_date
    df_out["run_id"] = run.run_id
    return df_out
