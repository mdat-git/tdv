# src/inspections_lakehouse/etl/etl_008_official_scope_master/silverize.py
from __future__ import annotations

import re
from typing import Optional

import pandas as pd


def _norm_col(c: str) -> str:
    # normalize headers for matching
    return re.sub(r"\s+", "", str(c).strip())


def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """
    Finds the first matching column name by normalized comparison.
    """
    norm_map = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm_map:
            return norm_map[key]
    return None


def _clean_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def silverize_scope_table(
    df_bronze: pd.DataFrame,
    *,
    run,
    asset_class: str,
    scope_list: str,
    source_system: str,
    source_file_saved: str,
) -> pd.DataFrame:
    """
    Canonical selection for the master scope tables.

    Required fields (from management sheet):
      - FLOC
      - Eq_OjType  (note: spelling looks like that; we match flexibly)

    Output is intentionally simple but extensible.
    """
    df = df_bronze.copy()

    floc_col = _find_col(df, "FLOC", "Floc", "FLOC_ID", "FLOCID")
    obj_col = _find_col(df, "Eq_OjType", "Eq_ObjType", "EqObjType", "Eq_Oj_Type", "EqOjType", "EqType")

    if not floc_col:
        raise ValueError(f"Could not find FLOC column. Columns: {list(df.columns)}")
    if not obj_col:
        raise ValueError(f"Could not find Eq_OjType column. Columns: {list(df.columns)}")

    out = pd.DataFrame()
    out["floc"] = _clean_str(df[floc_col])
    out["object_type"] = _clean_str(df[obj_col])

    # metadata / descriptors
    out["asset_class"] = asset_class
    out["scope_list"] = scope_list

    # lineage
    out["source_system"] = source_system
    out["source_file_saved"] = source_file_saved
    out["run_date"] = run.run_date
    out["run_id"] = run.run_id

    # drop empty flocs
    out = out[out["floc"].notna() & (out["floc"] != "")].reset_index(drop=True)
    return out


def build_floc_object_type_dim(df_union: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a consolidated floc → object_type table from the official scope union.

    If multiple object types appear for a floc, choose deterministically:
      EZ_POLE > ET_POLE > ED_POLE > first-seen
    Also retain joined set for forensics.
    """
    df = df_union.copy()
    df["floc"] = df["floc"].astype("string").str.strip()
    df["object_type"] = df["object_type"].astype("string").str.strip()

    def join_unique(s: pd.Series) -> str | None:
        vals = [v for v in s.dropna().astype(str).str.strip().tolist() if v]
        seen = set()
        uniq = []
        for v in vals:
            if v not in seen:
                seen.add(v)
                uniq.append(v)
        return ";".join(uniq) if uniq else None

    g = df.groupby("floc", dropna=False)

    out = g.agg(
        object_type_values=("object_type", join_unique),
        scope_lists=("scope_list", join_unique),
        asset_classes=("asset_class", join_unique),
        row_count=("floc", "size"),
    ).reset_index()

    def pick_best(values: str | None) -> str | None:
        if not values:
            return None
        parts = values.split(";")
        if "EZ_POLE" in parts:
            return "EZ_POLE"
        if "ET_POLE" in parts:
            return "ET_POLE"
        if "ED_POLE" in parts:
            return "ED_POLE"
        return parts[0] if parts else None

    out["object_type"] = out["object_type_values"].apply(pick_best)
    return out
