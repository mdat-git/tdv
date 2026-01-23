from __future__ import annotations
import re
import pandas as pd

_SCOPE_PKG_RE = re.compile(r"^Scope Package\s*#\s*(\d+)\s*$", re.IGNORECASE)

def _strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out

def _find_pkg_cols(cols: list[str]) -> list[str]:
    hits = []
    for c in cols:
        m = _SCOPE_PKG_RE.match(str(c).strip())
        if m:
            hits.append((int(m.group(1)), c))
    hits.sort(key=lambda t: t[0])
    return [c for _, c in hits]

def normalize_scope_id_distribution(s: pd.Series) -> pd.Series:
    s2 = s.astype("string").str.strip().replace({"": None, "nan": None, "NaT": None})
    return s2.fillna("COMP")

def canonicalize_for_key(df_raw: pd.DataFrame, *, sheet_name: str) -> pd.DataFrame:
    """
    Returns a minimal canonical df with:
      - scope_id
      - floc
      - scope_removal_date (best-effort)
      - visit_no (Transmission only, else NA)
    Used by delta + silver.
    """
    df = _strip_cols(df_raw)

    if sheet_name.lower() == "distribution":
        out = pd.DataFrame()
        out["floc"] = df["FLOC"].astype("string").str.strip()
        out["scope_id"] = normalize_scope_id_distribution(df["SCOPE_ID"])
        out["scope_removal_date"] = pd.to_datetime(df.get("SCOPE_REMOVAL_DATE"), errors="coerce")
        out["visit_no"] = pd.Series([pd.NA] * len(out), dtype="Int64")
        return out

    if sheet_name.lower() == "transmission":
        pkg_cols = _find_pkg_cols(list(df.columns))
        if not pkg_cols:
            raise ValueError("Transmission: no Scope Package #N columns found.")

        id_cols = [c for c in df.columns if c not in pkg_cols]

        melted = df.melt(
            id_vars=id_cols,
            value_vars=pkg_cols,
            var_name="scope_package_slot",
            value_name="scope_id",
        )
        melted["scope_id"] = melted["scope_id"].astype("string").str.strip()
        melted = melted[melted["scope_id"].notna() & (melted["scope_id"] != "")].copy()

        out = pd.DataFrame()
        out["floc"] = melted["FLOC"].astype("string").str.strip()
        out["scope_id"] = melted["scope_id"]
        out["scope_removal_date"] = pd.to_datetime(melted.get("Date_Removed"), errors="coerce")
        out["visit_no"] = (
            melted["scope_package_slot"].astype("string").str.extract(_SCOPE_PKG_RE, expand=False).astype("Int64")
        )
        return out

    raise ValueError(f"Unknown sheet_name: {sheet_name}")
