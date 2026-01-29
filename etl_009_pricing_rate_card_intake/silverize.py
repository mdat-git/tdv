# src/inspections_lakehouse/etl/etl_009_pricing_rate_card_intake/silverize.py
from __future__ import annotations

import re
from typing import Dict, Any

import pandas as pd


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in out.columns]
    return out


def _clean_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _nul(s: pd.Series) -> pd.Series:
    s2 = s.astype("string").str.strip()
    return s2.where(s2 != "", pd.NA)


def _to_float(s: pd.Series) -> pd.Series:
    s2 = _nul(s)
    return pd.to_numeric(s2, errors="coerce")


def _to_int(s: pd.Series, default: int = 0) -> pd.Series:
    x = pd.to_numeric(_nul(s), errors="coerce").fillna(default)
    return x.astype("int64")


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def silverize_rate_card(df_raw: pd.DataFrame, *, run, source_system: str) -> pd.DataFrame:
    """
    Produces a normalized rate card table.

    Expected target columns (silver):
      vendor
      billing_bucket
      object_type (nullable)
      voltage (nullable)
      uom (default FLOC)
      unit_price
      effective_start_date (nullable)
      effective_end_date (nullable)
      priority (int; default based on specificity if missing)
      rule_name (nullable)
      + lineage
    """
    df = _clean_cols(df_raw)

    # Map “whatever finance calls it” -> canonical
    col_vendor = _pick_col(df, ["vendor", "contractor", "vendor_name"])
    col_bucket = _pick_col(df, ["billing_bucket", "program", "program_acronym", "bucket"])
    col_obj = _pick_col(df, ["object_type", "eq_objtype", "eq_obj_type", "object"])
    col_kv = _pick_col(df, ["voltage", "kv", "kV", "voltage_kv"])
    col_price = _pick_col(df, ["unit_price", "price", "rate", "unit rate", "unit_rate"])
    col_uom = _pick_col(df, ["uom", "unit", "unit_of_measure"])
    col_eff_start = _pick_col(df, ["effective_start_date", "start_date", "effective_start"])
    col_eff_end = _pick_col(df, ["effective_end_date", "end_date", "effective_end"])
    col_priority = _pick_col(df, ["priority", "rule_priority"])
    col_rule = _pick_col(df, ["rule_name", "rule", "rate_name", "pricing_rule"])

    if col_vendor is None or col_bucket is None or col_price is None:
        missing = [x for x, c in [("vendor", col_vendor), ("billing_bucket", col_bucket), ("unit_price", col_price)] if c is None]
        raise ValueError(f"Rate card is missing required fields: {missing}. Add them or update column mapping.")

    out = pd.DataFrame()
    out["vendor"] = _clean_str(df[col_vendor])
    out["billing_bucket"] = _clean_str(df[col_bucket])

    out["object_type"] = _clean_str(df[col_obj]) if col_obj else pd.Series(pd.NA, index=df.index, dtype="string")
    out["voltage"] = _clean_str(df[col_kv]) if col_kv else pd.Series(pd.NA, index=df.index, dtype="string")
    out["uom"] = _clean_str(df[col_uom]) if col_uom else "FLOC"
    out["unit_price"] = _to_float(df[col_price])

    out["effective_start_date"] = _nul(df[col_eff_start]) if col_eff_start else pd.Series(pd.NA, index=df.index, dtype="string")
    out["effective_end_date"] = _nul(df[col_eff_end]) if col_eff_end else pd.Series(pd.NA, index=df.index, dtype="string")
    out["rule_name"] = _clean_str(df[col_rule]) if col_rule else pd.Series(pd.NA, index=df.index, dtype="string")

    if col_priority:
        out["priority"] = _to_int(df[col_priority], default=0)
    else:
        # default specificity-based priority: base=10, +10 if obj specified, +10 if voltage specified
        out["priority"] = 10
        out.loc[out["object_type"].fillna("").ne(""), "priority"] += 10
        out.loc[out["voltage"].fillna("").ne(""), "priority"] += 10

    # Drop totally unusable rows
    out = out.dropna(subset=["vendor", "billing_bucket", "unit_price"])

    # Lineage
    out["rate_run_date"] = run.run_date
    out["rate_run_id"] = run.run_id
    out["rate_source_system"] = source_system

    # Normalize blanks to NA
    for c in ["object_type", "voltage", "uom", "rule_name", "effective_start_date", "effective_end_date"]:
        out[c] = _nul(out[c])

    return out.reset_index(drop=True)
