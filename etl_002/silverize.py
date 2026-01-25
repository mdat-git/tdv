from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog


_SCOPE_PKG_RE = re.compile(r"^Scope Package\s*#\s*(\d+)\s*$", re.IGNORECASE)

EXPECTED_EVENT_COLS = [
    "floc",
    "scope_id",
    "event_type",
    "event_effective_date",
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


def _col_lookup(df: pd.DataFrame) -> dict[str, str]:
    return {str(c).strip().lower(): str(c).strip() for c in df.columns}


def _get_col_any(df: pd.DataFrame, candidates: list[str]) -> str:
    lookup = _col_lookup(df)
    for name in candidates:
        hit = lookup.get(name.lower())
        if hit:
            return hit
    raise KeyError(f"Missing required column. Tried: {candidates}. Available: {list(df.columns)}")


def _maybe_get_col_any(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    lookup = _col_lookup(df)
    for name in candidates:
        hit = lookup.get(name.lower())
        if hit:
            return hit
    return None


def _as_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _parse_dt(s: pd.Series) -> pd.Series:
    s2 = s.astype("string").replace({"": None, "nan": None, "NaT": None})
    return pd.to_datetime(s2, errors="coerce")


def _find_scope_package_cols(df: pd.DataFrame) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for c in df.columns:
        m = _SCOPE_PKG_RE.match(str(c).strip())
        if m:
            hits.append((int(m.group(1)), c))
    hits.sort(key=lambda t: t[0])
    return hits


# ----------------------------
# Builders
# ----------------------------
def build_events_distribution_removal(
    df_raw: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
) -> pd.DataFrame:
    df = _strip_cols(df_raw)

    c_floc = _get_col_any(df, ["FLOC"])
    c_scope = _get_col_any(df, ["SCOPE_ID", "Scope ID"])
    c_removed = _get_col_any(df, ["SCOPE_REMOVAL_DATE", "SCOPE_REMOVAL DATE", "Scope Removal Date"])

    out = pd.DataFrame(index=df.index)
    out["floc"] = _as_str(df[c_floc])

    scope = _as_str(df[c_scope]).replace({"": None, "nan": None, "NaT": None})
    out["scope_id"] = scope.fillna("COMP")

    out["event_type"] = "removal"
    out["event_effective_date"] = _parse_dt(df[c_removed])

    out["visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    out["scope_floc_key"] = out["scope_id"].astype("string") + "|" + out["floc"].astype("string")

    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = "Distribution"

    # keep only rows with an effective date + key
    out = out[
        out["event_effective_date"].notna()
        & out["floc"].notna() & (out["floc"] != "")
        & out["scope_id"].notna() & (out["scope_id"] != "")
    ].copy()

    return out


def build_events_transmission_removal(
    df_raw: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
) -> pd.DataFrame:
    df = _strip_cols(df_raw)

    c_floc = _get_col_any(df, ["FLOC"])
    c_removed = _get_col_any(df, ["Date_Removed", "Date Removed", "DATE_REMOVED"])

    pkg_hits = _find_scope_package_cols(df)
    if not pkg_hits:
        raise ValueError("Transmission removals: no 'Scope Package #N' columns found.")

    pkg_cols = [c for _, c in pkg_hits]
    id_cols = [c for c in df.columns if c not in pkg_cols]

    melted = df.melt(
        id_vars=id_cols,
        value_vars=pkg_cols,
        var_name="scope_package_slot",
        value_name="scope_id",
    )

    melted["scope_id"] = _as_str(melted["scope_id"])
    melted = melted[melted["scope_id"].notna() & (melted["scope_id"] != "")].copy()

    out = pd.DataFrame(index=melted.index)
    out["floc"] = _as_str(melted[c_floc])
    out["scope_id"] = _as_str(melted["scope_id"])

    out["event_type"] = "removal"
    out["event_effective_date"] = _parse_dt(melted[c_removed])

    out["visit_no"] = (
        melted["scope_package_slot"]
        .astype("string")
        .str.extract(_SCOPE_PKG_RE, expand=False)
        .astype("Int64")
    )

    out["scope_floc_key"] = out["scope_id"].astype("string") + "|" + out["floc"].astype("string")

    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = "Transmission"

    out = out[
        out["event_effective_date"].notna()
        & out["floc"].notna() & (out["floc"] != "")
        & out["scope_id"].notna() & (out["scope_id"] != "")
    ].copy()

    return out


def build_events_move_to_helo(
    df_raw: pd.DataFrame,
    *,
    vendor: str,
    run: RunLog,
    source_file_saved: str,
    sheet_name: str,
) -> pd.DataFrame:
    df = _strip_cols(df_raw)

    c_floc = _get_col_any(df, ["FLOC"])
    c_scope = _get_col_any(df, ["SCOPE_ID", "Scope ID"])

    out = pd.DataFrame(index=df.index)
    out["floc"] = _as_str(df[c_floc])

    scope = _as_str(df[c_scope]).replace({"": None, "nan": None, "NaT": None})
    out["scope_id"] = scope.fillna("COMP")

    out["event_type"] = "move_to_helo"
    # per your requirement: effective as-of ingestion
    out["event_effective_date"] = pd.to_datetime(run.partitions["run_date"], errors="coerce")

    out["visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    out["scope_floc_key"] = out["scope_id"].astype("string") + "|" + out["floc"].astype("string")

    out["vendor"] = vendor
    out["run_date"] = run.partitions["run_date"]
    out["run_id"] = run.partitions["run_id"]
    out["source_file_saved"] = source_file_saved
    out["source_sheet"] = sheet_name

    out = out[
        out["floc"].notna() & (out["floc"] != "")
        & out["scope_id"].notna() & (out["scope_id"] != "")
    ].copy()

    return out


# ----------------------------
# Writer
# ----------------------------
def write_events_line_and_header(
    *,
    events_df: pd.DataFrame,
    vendor: str,
    event_type: str,
    run: RunLog,
    source_file_saved: str,
) -> None:
    # Schema lock
    for c in ["floc", "scope_id", "scope_floc_key", "vendor", "event_type", "source_sheet"]:
        if c in events_df.columns:
            events_df[c] = events_df[c].astype("string")

    events_df = enforce_schema(events_df, EXPECTED_EVENT_COLS, context=f"scope_events:{vendor}:{event_type}")
    run.metrics["scope_events__schema_missing"] = events_df.attrs["schema_missing"]
    run.metrics["scope_events__schema_extra"] = events_df.attrs["schema_extra"]

    # LINE
    line_hist_dir = paths.local_dir(
        "silver",
        "scope_events_line",
        "HISTORY",
        partitions={"vendor": vendor, "event_type": event_type, **run.partitions},
        ensure=True,
    )
    line_hist_file = write_dataset(events_df, line_hist_dir, basename="data")

    line_curr_dir = paths.local_dir(
        "silver",
        "scope_events_line",
        "CURRENT",
        partitions={"vendor": vendor, "event_type": event_type},
        ensure=True,
    )
    line_curr_file = write_dataset(events_df, line_curr_dir, basename="data")

    # HEADER
    header_df = pd.DataFrame(
        [
            {
                "vendor": vendor,
                "event_type": event_type,
                "run_date": run.partitions["run_date"],
                "run_id": run.partitions["run_id"],
                "source_file_saved": source_file_saved,
                "row_count_events": int(len(events_df)),
            }
        ]
    )

    header_hist_dir = paths.local_dir(
        "silver",
        "scope_events_header",
        "HISTORY",
        partitions={"vendor": vendor, "event_type": event_type, **run.partitions},
        ensure=True,
    )
    header_hist_file = write_dataset(header_df, header_hist_dir, basename="data")

    header_curr_dir = paths.local_dir(
        "silver",
        "scope_events_header",
        "CURRENT",
        partitions={"vendor": vendor, "event_type": event_type},
        ensure=True,
    )
    header_curr_file = write_dataset(header_df, header_curr_dir, basename="data")

    run.metrics.update(
        {
            "scope_events__rows": int(len(events_df)),
            "scope_events__line_history_file": str(line_hist_file),
            "scope_events__line_current_file": str(line_curr_file),
            "scope_events__header_history_file": str(header_hist_file),
            "scope_events__header_current_file": str(header_curr_file),
        }
    )
