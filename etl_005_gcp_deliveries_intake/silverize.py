# src/inspections_lakehouse/etl/etl_005_gcp_deliveries_intake/canonicalize.py
from __future__ import annotations

import pandas as pd

from inspections_lakehouse.util.vendors import validate_vendor

REQUIRED_SOURCE_COLS = ["DateUploaded", "flight_date", "vendor", "structureId", "folder", "imageCount"]


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")


def _parse_mm_dd_yyyy(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="%m-%d-%Y", errors="coerce").dt.date


def _parse_folder(folder: pd.Series) -> pd.DataFrame:
    first_token = folder.astype(str).str.split("_", n=1).str[0]
    scope_id = first_token.str.split("-", n=1).str[0]
    block_id = first_token.str.split("-", n=1).str[1]
    yyyymmdd = folder.astype(str).str.rsplit("_", n=1).str[-1]
    folder_date = pd.to_datetime(yyyymmdd, format="%Y%m%d", errors="coerce").dt.date
    return pd.DataFrame({"scope_id": scope_id, "block_id": block_id, "folder_date": folder_date})


def _dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["vendor", "floc", "folder"]
    df = df.sort_values(
        by=["date_uploaded", "flight_date", "run_date", "run_id"],
        ascending=[False, False, False, False],
        kind="mergesort",
    )
    return df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)


def build_silver_evidence(
    *,
    df_bronze: pd.DataFrame,
    run,
    source_file_saved: str,
    source_system: str,
    asset_class: str,
) -> pd.DataFrame:
    asset_class = asset_class.lower().strip()
    if asset_class not in {"distribution", "transmission"}:
        raise ValueError(f"asset_class must be 'distribution' or 'transmission', got: {asset_class}")

    _require_columns(df_bronze, REQUIRED_SOURCE_COLS)
    df = df_bronze.copy()

    for v in sorted(set(df["vendor"].astype(str).tolist())):
        validate_vendor(v)

    df["asset_class"] = asset_class

    df["date_uploaded"] = _parse_mm_dd_yyyy(df["DateUploaded"])
    df["flight_date"] = _parse_mm_dd_yyyy(df["flight_date"])
    df["floc"] = df["structureId"].astype(str)
    df["folder"] = df["folder"].astype(str)
    df["image_count"] = pd.to_numeric(df["imageCount"], errors="coerce").fillna(0).astype(int)

    parsed = _parse_folder(df["folder"])
    df["scope_id"] = parsed["scope_id"].astype(str)
    df["block_id"] = parsed["block_id"].astype(str)
    df["folder_date"] = parsed["folder_date"]

    df["scope_floc_key"] = df["scope_id"].astype(str) + "|" + df["floc"].astype(str)
    df["has_deliveries_evidence"] = df["image_count"] > 0

    df["source_system"] = source_system
    df["run_date"] = run.run_date
    df["run_id"] = run.run_id
    df["source_file_saved"] = source_file_saved

    out_cols = [
        "asset_class",
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
    df_out = _dedupe_latest(df[out_cols].copy())
    return df_out
