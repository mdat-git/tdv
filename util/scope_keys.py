from __future__ import annotations

import pandas as pd

SCOPE_COL = "SCOPE_ID"
FLOC_COL = "FLOC"
REMOVAL_COL = "SCOPE_REMOVAL_DATE"

def normalize_scope_id(series: pd.Series) -> pd.Series:
    """
    Business rule:
    - blank/NULL SCOPE_ID => 'COMP' (compliance scope bucket)
    """
    s = series.astype("string").fillna("").str.strip()
    return s.mask(s.eq(""), "COMP")

def normalize_floc(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()

def add_business_key(df: pd.DataFrame, key_col: str = "_key") -> pd.DataFrame:
    """
    Adds a stable business key column based on (SCOPE_ID, FLOC),
    with SCOPE_ID blank => COMP.
    """
    out = df.copy()
    out[SCOPE_COL] = normalize_scope_id(out[SCOPE_COL])
    out[FLOC_COL] = normalize_floc(out[FLOC_COL])
    out[key_col] = out[SCOPE_COL] + "|" + out[FLOC_COL]
    return out
