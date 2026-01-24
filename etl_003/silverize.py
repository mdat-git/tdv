from __future__ import annotations

from typing import Optional

import pandas as pd

from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog


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
    lookup = {str(c).strip().lower(): str(c).strip() for c in df.columns}
    hit = lookup.get(name.lower())
    if not hit:
        raise KeyError(f"Missing required column '{name}'. Available columns: {list(df.columns)}")
    return hit


def _as_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def build_silver_line(
    df_bronze: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
    source_sheet: str,
) -> pd.DataFrame:
    df = _strip_cols(df_bronze)
    c_floc = _get_col(df, "FLOC")
    c_scope = _get_col(df, "SCOPE_ID")

    out = pd.DataFrame(index=df.index)

    out["floc"] = _as_str(df[c_floc])
    out["scope_id"] = _as_str(df[c_scope])

    # HELO snapshot: always active
    out["scope_removal_date"] = pd.NaT
    out["is_active"] = True

    # not applicable for HELO
    out["visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")

    # key
    out["scope_floc_key"] = out["scope_id"].astype("string") + "|" + out["floc"].astype("string")

    # metadata
    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = source_sheet

    # drop blank key rows if any
    out = out[(out["floc"].notna()) & (out["floc"] != "") & (out["scope_id"].notna()) & (out["scope_id"] != "")].copy()

    return out


def build_silver_header(
    *,
    vendor: str,
    run: RunLog,
    dataset_name: str,
    sheet_name: str,
    source_file_saved: str,
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
                "run_date": run.partitions["run_date"],
                "run_id": run.partitions["run_id"],
                "source_file_saved": source_file_saved,
                "row_count_raw": int(row_count_raw),
                "col_count_raw": int(col_count_raw),
                "row_count_silver_line": int(row_count_line),
            }
        ]
    )


def write_silver_line_and_header(
    *,
    df_bronze: pd.DataFrame,
    dataset_name: str,   # e.g. "helo_scope_distribution" | "helo_scope_transmission"
    sheet_name: str,     # "Distribution" | "Transmission"
    vendor: str,
    run: RunLog,
    source_file_saved: str,
) -> None:
    line_df = build_silver_line(
        df_bronze,
        vendor=vendor,
        run=run,
        source_file_saved=source_file_saved,
        source_sheet=sheet_name,
    )

    # Parquet safety: force string cols
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

    run.metrics.update(
        {
            f"{dataset_name}__silver_line_rows": int(len(line_df)),
            f"{dataset_name}__silver_line_history_file": str(line_hist_file),
            f"{dataset_name}__silver_line_current_file": str(line_curr_file),
            f"{dataset_name}__silver_header_history_file": str(header_hist_file),
            f"{dataset_name}__silver_header_current_file": str(header_curr_file),
        }
    )
