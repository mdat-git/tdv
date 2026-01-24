from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog


# ----------------------------
# Regex for Transmission packages
# ----------------------------
_SCOPE_PKG_RE = re.compile(r"^Scope Package\s*#\s*(\d+)\s*$", re.IGNORECASE)


# ----------------------------
# Tiny helpers
# ----------------------------
EXPECTED_LINE_COLS = [
    "floc",
    "scope_id",
    "scope_removal_date",
    "is_active",
    "visit_no",
    "scope_floc_key",
    "vendor",
    "run_date",
    "run_id",
    "source_file_saved",
    "source_sheet",
]


def enforce_schema(df: pd.DataFrame, expected: list[str], *, context: str) -> pd.DataFrame:
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]

    for c in missing:
        df[c] = pd.NA

    df2 = df[expected].copy()
    df2.attrs["schema_missing"] = missing
    df2.attrs["schema_extra"] = extra
    df2.attrs["schema_context"] = context
    return df2


def _strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _get_col(df: pd.DataFrame, name: str) -> str:
    """
    Case-insensitive column lookup with helpful error.
    """
    lookup = {c.lower(): c for c in df.columns}
    hit = lookup.get(name.lower())
    if not hit:
        raise KeyError(f"Missing required column '{name}'. Available columns: {list(df.columns)}")
    return hit


def _maybe_get_col(df: pd.DataFrame, name: str) -> Optional[str]:
    lookup = {c.lower(): c for c in df.columns}
    return lookup.get(name.lower())


def _as_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _parse_dt(s: pd.Series) -> pd.Series:
    s2 = s.astype("string").replace({"": None, "nan": None, "NaT": None})
    return pd.to_datetime(s2, errors="coerce")


def _is_active(removal_dt: pd.Series) -> pd.Series:
    return removal_dt.isna()


def _find_scope_package_cols(df: pd.DataFrame) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for c in df.columns:
        m = _SCOPE_PKG_RE.match(str(c).strip())
        if m:
            hits.append((int(m.group(1)), c))
    hits.sort(key=lambda t: t[0])
    return hits


# ----------------------------
# Canonical builders
# ----------------------------
def build_silver_line_distribution(
    df_bronze: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
) -> pd.DataFrame:
    """
    Distribution (canonical):
      FLOC, SCOPE_REMOVAL_DATE, SCOPE_ID (blank => COMP)

    Output:
      floc, scope_id, scope_removal_date, is_active, visit_no, scope_floc_key, + metadata
    """
    df = _strip_cols(df_bronze)

    c_floc = _get_col(df, "FLOC")
    c_scope = _get_col(df, "SCOPE_ID")
    c_removed = _maybe_get_col(df, "SCOPE_REMOVAL_DATE")

    out = pd.DataFrame(index=df.index)

    out["floc"] = _as_str(df[c_floc])

    scope = _as_str(df[c_scope]).replace({"": None, "nan": None, "NaT": None})
    out["scope_id"] = scope.fillna("COMP")

    if c_removed:
        out["scope_removal_date"] = _parse_dt(df[c_removed])
    else:
        out["scope_removal_date"] = pd.NaT

    out["is_active"] = _is_active(out["scope_removal_date"])
    out["visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")

    out["scope_floc_key"] = out["scope_id"] + "|" + out["floc"]

    # metadata
    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = "Distribution"

    return out


def build_silver_line_transmission(
    df_bronze: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
) -> pd.DataFrame:
    """
    Transmission:
      FLOC, Date_Removed, Scope Package #N

    Transform:
      melt Scope Package #N -> scope_id (drop blanks)

    Output:
      floc, scope_id, scope_removal_date, is_active, visit_no, scope_floc_key, + metadata
    """
    df = _strip_cols(df_bronze)

    c_floc = _get_col(df, "FLOC")
    c_removed = _maybe_get_col(df, "Date_Removed")

    pkg_hits = _find_scope_package_cols(df)
    if not pkg_hits:
        raise ValueError("Transmission: no 'Scope Package #N' columns found to unpivot.")

    pkg_cols = [c for _, c in pkg_hits]
    id_cols = [c for c in df.columns if c not in pkg_cols]

    melted = df.melt(
        id_vars=id_cols,
        value_vars=pkg_cols,
        var_name="scope_package_slot",
        value_name="scope_id",
    )

    # drop placeholder blanks (DO NOT fill with COMP)
    melted["scope_id"] = _as_str(melted["scope_id"])
    melted = melted[melted["scope_id"].notna() & (melted["scope_id"] != "")].copy()

    out = pd.DataFrame(index=melted.index)

    out["floc"] = _as_str(melted[c_floc])
    out["scope_id"] = _as_str(melted["scope_id"])

    if c_removed:
        out["scope_removal_date"] = _parse_dt(melted[c_removed])
    else:
        out["scope_removal_date"] = pd.NaT

    out["is_active"] = _is_active(out["scope_removal_date"])

    out["visit_no"] = (
        melted["scope_package_slot"]
        .astype("string")
        .str.extract(_SCOPE_PKG_RE, expand=False)
        .astype("Int64")
    )

    # Key does NOT include visit_no (scope_id is unique). Keep visit_no anyway.
    out["scope_floc_key"] = out["scope_id"] + "|" + out["floc"]

    # metadata
    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = "Transmission"

    return out


def build_silver_header(
    *,
    vendor: str,
    run: RunLog,
    dataset_name: str,
    sheet_name: str,
    source_file_saved: str,
    release: Optional[str],
    row_count_raw: int,
    col_count_raw: int,
    row_count_line: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "vendor": vendor,
                "dataset": dataset_name,
                "source_sheet": sheet_name,
                "release": release,
                "run_date": run.partitions["run_date"],
                "run_id": run.partitions["run_id"],
                "source_file_saved": source_file_saved,
                "row_count_raw": int(row_count_raw),
                "col_count_raw": int(col_count_raw),
                "row_count_silver_line": int(row_count_line),
            }
        ]
    )


# ----------------------------
# Writer: line + header
# ----------------------------
def write_silver_line_and_header(
    *,
    df_bronze: pd.DataFrame,
    dataset_name: str,      # e.g. "scope_release_distribution" or "scope_release_transmission"
    sheet_name: str,        # "Distribution" | "Transmission"
    vendor: str,
    run: RunLog,
    source_file_saved: str,
    release: Optional[str] = None,
) -> None:
    # Build line
    if sheet_name.lower() == "distribution":
        line_df = build_silver_line_distribution(
            df_bronze, vendor=vendor, run=run, source_file_saved=source_file_saved
        )
    elif sheet_name.lower() == "transmission":
        line_df = build_silver_line_transmission(
            df_bronze, vendor=vendor, run=run, source_file_saved=source_file_saved
        )
    else:
        raise ValueError(f"Unknown sheet_name '{sheet_name}' (expected Distribution/Transmission).")

    # Parquet safety: force key string cols
    for c in ["floc", "scope_id", "scope_floc_key", "vendor"]:
        if c in line_df.columns:
            line_df[c] = line_df[c].astype("string")

    line_df = enforce_schema(line_df, EXPECTED_LINE_COLS, context=f"{dataset_name}:{vendor}")
    run.metrics[f"{dataset_name}__schema_missing"] = line_df.attrs["schema_missing"]
    run.metrics[f"{dataset_name}__schema_extra"] = line_df.attrs["schema_extra"]

    # Write LINE
    line_hist_dir = paths.local_dir(
        "silver",
        f"{dataset_name}_line",
        "HISTORY",
        partitions={"vendor": vendor, **run.partitions},
        ensure=True,
    )
    line_hist_file = write_dataset(line_df, line_hist_dir, basename="data")

    line_curr_dir = paths.local_dir(
        "silver",
        f"{dataset_name}_line",
        "CURRENT",
        partitions={"vendor": vendor},
        ensure=True,
    )
    line_curr_file = write_dataset(line_df, line_curr_dir, basename="data")

    # Write HEADER
    header_df = build_silver_header(
        vendor=vendor,
        run=run,
        dataset_name=dataset_name,
        sheet_name=sheet_name,
        source_file_saved=source_file_saved,
        release=release,
        row_count_raw=len(df_bronze),
        col_count_raw=df_bronze.shape[1],
        row_count_line=len(line_df),
    )

    header_hist_dir = paths.local_dir(
        "silver",
        f"{dataset_name}_header",
        "HISTORY",
        partitions={"vendor": vendor, **run.partitions},
        ensure=True,
    )
    header_hist_file = write_dataset(header_df, header_hist_dir, basename="data")

    header_curr_dir = paths.local_dir(
        "silver",
        f"{dataset_name}_header",
        "CURRENT",
        partitions={"vendor": vendor},
        ensure=True,
    )
    header_curr_file = write_dataset(header_df, header_curr_dir, basename="data")

    # Metrics pointers
    run.metrics.update(
        {
            f"{dataset_name}__silver_line_rows": int(len(line_df)),
            f"{dataset_name}__silver_line_cols": int(line_df.shape[1]),
            f"{dataset_name}__silver_line_history_file": str(line_hist_file),
            f"{dataset_name}__silver_line_current_file": str(line_curr_file),
            f"{dataset_name}__silver_header_history_file": str(header_hist_file),
            f"{dataset_name}__silver_header_current_file": str(header_curr_file),
        }
    )
