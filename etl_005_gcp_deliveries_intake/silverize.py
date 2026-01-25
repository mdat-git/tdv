# src/inspections_lakehouse/etl/etl_005_gcp_deliveries_intake/canonicalize.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from inspections_lakehouse.util.vendors import validate_vendor


REQUIRED_SOURCE_COLS = [
    "DateUploaded",
    "flight_date",
    "vendor",
    "structureId",
    "folder",
    "imageCount",
]


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")


def _parse_mm_dd_yyyy(s: pd.Series) -> pd.Series:
    # Input looks like "01-15-2026"
    # Coerce invalid to NaT rather than blow up.
    return pd.to_datetime(s, format="%m-%d-%Y", errors="coerce").dt.date


def _parse_folder(folder: pd.Series) -> pd.DataFrame:
    """
    folder format is guaranteed:
      (SCOPE_ID)-(BLOCK_ID)_Assets_YYYYMMDD

    We derive:
      scope_id  = left of '-' in the first token before '_'
      block_id  = right of '-' in the first token before '_'
      folder_date = YYYYMMDD parsed as date
    """
    # first token: "(SCOPE_ID)-(BLOCK_ID)"
    first_token = folder.astype(str).str.split("_", n=1).str[0]

    scope_id = first_token.str.split("-", n=1).str[0]
    block_id = first_token.str.split("-", n=1).str[1]

    # suffix date after last "_" is "YYYYMMDD" (guaranteed)
    yyyymmdd = folder.astype(str).str.rsplit("_", n=1).str[-1]
    folder_date = pd.to_datetime(yyyymmdd, format="%Y%m%d", errors="coerce").dt.date

    return pd.DataFrame(
        {
            "scope_id": scope_id,
            "block_id": block_id,
            "folder_date": folder_date,
        }
    )


def _dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonical dedupe: one row per delivery folder (per vendor+floc+folder).
    Keep latest by DateUploaded, then flight_date, then run_date/run_id.
    """
    # These are stable identifiers for a batch
    key_cols = ["vendor", "floc", "folder"]

    # Sort latest-first
    df = df.sort_values(
        by=[
            "date_uploaded",
            "flight_date",
            "run_date",
            "run_id",
        ],
        ascending=[False, False, False, False],
        kind="mergesort",  # stable, deterministic
    )

    # Drop duplicates keeping first (latest)
    return df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)


def build_silver_evidence(
    *,
    df_bronze: pd.DataFrame,
    run,
    source_file_saved: str,
    source_system: str,
) -> pd.DataFrame:
    """
    Build canonical silver evidence lines from Bronze.

    Inputs: Bronze df (raw columns preserved + added lineage)
    Output: Silver canonical, joinable to ETL004 via scope_floc_key = scope_id|floc
    """
    _require_columns(df_bronze, REQUIRED_SOURCE_COLS)

    df = df_bronze.copy()

    # Validate vendors (prevents typos from polluting CURRENT paths)
    for v in sorted(set(df["vendor"].astype(str).tolist())):
        validate_vendor(v)

    # Rename to canonical names matching your other ETLs
    df["date_uploaded"] = _parse_mm_dd_yyyy(df["DateUploaded"])
    df["flight_date"] = _parse_mm_dd_yyyy(df["flight_date"])
    df["floc"] = df["structureId"].astype(str)
    df["folder"] = df["folder"].astype(str)

    # image_count int
    df["image_count"] = pd.to_numeric(df["imageCount"], errors="coerce").fillna(0).astype(int)

    # Parse folder into scope_id / block_id / folder_date
    parsed = _parse_folder(df["folder"])
    df["scope_id"] = parsed["scope_id"].astype(str)
    df["block_id"] = parsed["block_id"].astype(str)
    df["folder_date"] = parsed["folder_date"]

    # Business key
    df["scope_floc_key"] = df["scope_id"].astype(str) + "|" + df["floc"].astype(str)

    # Useful boolean for ETL006
    df["has_deliveries_evidence"] = df["image_count"] > 0

    # Lineage (standard)
    df["source_system"] = source_system
    df["run_date"] = run.run_date
    df["run_id"] = run.run_id
    df["source_file_saved"] = source_file_saved

    # Select canonical columns (keep folder + block_id for traceability)
    out_cols = [
        "vendor",
        "floc",
        "scope_id",
        "block_id",
        "scope_floc_key",
        "date_uploaded",
        "flight_date",
        "folder_date",
        "folder",
        "image_count",
        "has_deliveries_evidence",
        "source_system",
        "run_date",
        "run_id",
        "source_file_saved",
    ]
    df_out = df[out_cols].copy()

    # Deduplicate
    df_out = _dedupe_latest(df_out)

    return df_out
