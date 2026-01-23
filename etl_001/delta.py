from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.hashing import row_hashes


# ----------------------------
# IO helpers
# ----------------------------
def _read_snapshot(dir_path: Path) -> pd.DataFrame:
    p = dir_path / "data.parquet"
    c = dir_path / "data.csv"
    if p.exists():
        return pd.read_parquet(p)
    if c.exists():
        return pd.read_csv(c)
    raise FileNotFoundError(f"No data.parquet or data.csv found in: {dir_path}")


def _latest_prior_history_dir(dataset: str, vendor: str, current_run_id: str) -> Optional[Path]:
    """
    Find latest prior snapshot for this vendor.
    Chooses by filesystem modified time.
    """
    base = paths.local_dir("bronze", dataset, "HISTORY", partitions={"vendor": vendor}, ensure=True)

    candidates: list[Path] = []
    for f in base.rglob("data.parquet"):
        candidates.append(f.parent)
    for f in base.rglob("data.csv"):
        candidates.append(f.parent)

    # exclude current run
    candidates = [d for d in candidates if f"run_id={current_run_id}" not in str(d)]
    if not candidates:
        return None

    return max(candidates, key=lambda d: d.stat().st_mtime)


# ----------------------------
# Canonicalization for delta
# ----------------------------
_SCOPE_PKG_RE = re.compile(r"^Scope Package\s*#\s*(\d+)\s*$", re.IGNORECASE)


def _strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        c = cols.get(cand.lower())
        if c:
            return c
    return None


def _find_scope_package_cols(df: pd.DataFrame) -> list[tuple[int, str]]:
    """
    Returns [(1, 'Scope Package #1'), (2, 'Scope Package #2'), ...] sorted by N.
    """
    hits: list[tuple[int, str]] = []
    for c in df.columns:
        m = _SCOPE_PKG_RE.match(str(c).strip())
        if m:
            hits.append((int(m.group(1)), c))
    hits.sort(key=lambda t: t[0])
    return hits


def _parse_removal_dt(df: pd.DataFrame, *, mode: str) -> pd.Series:
    """
    Best-effort parse removal date for is_active flag.
    We support both "old" and "new" naming.
    """
    if mode == "distribution":
        cand = _first_existing_col(df, ["SCOPE_REMOVAL_DATE", "Scope Removal Date", "Date_Removed", "DATE_REMOVED"])
    else:
        cand = _first_existing_col(df, ["Date_Removed", "DATE_REMOVED", "SCOPE_REMOVAL_DATE", "Scope Removal Date"])

    if not cand:
        # no removal date column => assume active
        return pd.Series([pd.NaT] * len(df), index=df.index)

    s = df[cand].astype("string").replace({"": None, "nan": None, "NaT": None})
    return pd.to_datetime(s, errors="coerce")


def _is_active_from_dt(dt: pd.Series) -> pd.Series:
    return dt.isna()


def _distribution_keyed(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Distribution canonicalization for delta:
      - scope_id blank => COMP
      - key = scope_id|floc
    """
    df = _strip_cols(df_raw)

    floc_col = _first_existing_col(df, ["FLOC", "FLOC_ID"])
    scope_col = _first_existing_col(df, ["SCOPE_ID", "Scope ID"])

    if not floc_col or not scope_col:
        raise ValueError(f"Distribution delta: missing required columns. Found cols={list(df.columns)[:50]}")

    out = df.copy()
    out["_floc"] = out[floc_col].astype("string").str.strip()
    out["_scope_id"] = out[scope_col].astype("string").str.strip().replace({"": None, "nan": None, "NaT": None})
    out["_scope_id"] = out["_scope_id"].fillna("COMP")

    removal_dt = _parse_removal_dt(out, mode="distribution")
    out["_removal_dt"] = removal_dt
    out["_is_active"] = _is_active_from_dt(removal_dt)

    out["_visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    out["_key"] = out["_scope_id"] + "|" + out["_floc"]

    return out


def _transmission_keyed(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transmission canonicalization for delta:
      - unpivot Scope Package #N into one scope_id column
      - drop blank package rows (placeholders)
      - key = scope_id|floc|visit_no
    Works even if you previously renamed package #1 to SCOPE_ID (fallback behavior).
    """
    df = _strip_cols(df_raw)
    pkg_hits = _find_scope_package_cols(df)

    floc_col = _first_existing_col(df, ["FLOC", "FLOC_ID"])
    if not floc_col:
        raise ValueError("Transmission delta: missing FLOC/FLOC_ID column.")

    # Case A: preferred (Scope Package #N exists) => melt
    if pkg_hits:
        pkg_cols = [c for _, c in pkg_hits]
        id_cols = [c for c in df.columns if c not in pkg_cols]

        melted = df.melt(
            id_vars=id_cols,
            value_vars=pkg_cols,
            var_name="_scope_package_slot",
            value_name="_scope_id",
        )

        melted["_scope_id"] = melted["_scope_id"].astype("string").str.strip()
        melted = melted[melted["_scope_id"].notna() & (melted["_scope_id"] != "")].copy()

        # extract visit_no from the slot name
        melted["_visit_no"] = (
            melted["_scope_package_slot"]
            .astype("string")
            .str.extract(_SCOPE_PKG_RE, expand=False)
            .astype("Int64")
        )

        melted["_floc"] = melted[floc_col].astype("string").str.strip()

        removal_dt = _parse_removal_dt(melted, mode="transmission")
        melted["_removal_dt"] = removal_dt
        melted["_is_active"] = _is_active_from_dt(removal_dt)

        # key includes visit_no to avoid collisions if #1/#2 become real
        melted["_key"] = melted["_scope_id"].astype("string") + "|" + melted["_floc"].astype("string")

        return melted

    # Case B: fallback (someone renamed Scope Package #1 -> SCOPE_ID earlier)
    scope_col = _first_existing_col(df, ["SCOPE_ID", "Scope ID"])
    if not scope_col:
        raise ValueError("Transmission delta: no Scope Package #N and no SCOPE_ID/Scope ID fallback found.")

    out = df.copy()
    out["_floc"] = out[floc_col].astype("string").str.strip()
    out["_scope_id"] = out[scope_col].astype("string").str.strip()

    # IMPORTANT: transmission blanks are placeholders -> drop (NOT COMP fill)
    out = out[out["_scope_id"].notna() & (out["_scope_id"] != "")].copy()

    removal_dt = _parse_removal_dt(out, mode="transmission")
    out["_removal_dt"] = removal_dt
    out["_is_active"] = _is_active_from_dt(removal_dt)

    out["_visit_no"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    out["_key"] = out["_scope_id"].astype("string") + "|" + out["_floc"].astype("string")


    return out


def _keyed(df_raw: pd.DataFrame, *, dataset: str) -> pd.DataFrame:
    mode = "transmission" if "transmission" in dataset.lower() else "distribution"
    return _transmission_keyed(df_raw) if mode == "transmission" else _distribution_keyed(df_raw)


def _ensure_unique_keys(df: pd.DataFrame, context: str) -> None:
    if df["_key"].duplicated().any():
        sample = df.loc[df["_key"].duplicated(keep=False), "_key"].value_counts().head(10).to_dict()
        raise ValueError(f"Duplicate delta keys in {context}. Sample: {sample}")


# ----------------------------
# Main delta
# ----------------------------
def compute_key_delta(
    *,
    dataset: str,
    vendor: str,
    run_partitions: dict[str, str],
) -> Tuple[dict, Optional[Path]]:
    """
    Writes HISTORY-only:
      silver/<dataset>_delta/HISTORY/vendor=.../run_date=.../run_id=.../{added,removed,updated}.parquet|csv

    Delta semantics:
      ADDED:
        - new active keys
        - reactivated (previous inactive -> current active)
      REMOVED:
        - soft removal (prev active -> cur inactive)
        - missing removal (prev active -> key disappears)
      UPDATED:
        - active in both AND non-key attrs changed (row-hash signature)
    """
    run_id = run_partitions["run_id"]

    cur_dir = paths.local_dir("bronze", dataset, "HISTORY", partitions={"vendor": vendor, **run_partitions}, ensure=False)
    prev_dir = _latest_prior_history_dir(dataset, vendor, run_id)

    if prev_dir is None:
        return (
            {
                "delta_mode": "keyed_by_dataset",
                "delta_previous_found": False,
                "delta_current_dir": str(cur_dir),
                "added_rows": 0,
                "removed_rows": 0,
                "updated_rows": 0,
                "note": "No prior snapshot found; delta skipped.",
            },
            None,
        )

    cur_raw = _read_snapshot(cur_dir)
    prev_raw = _read_snapshot(prev_dir)

    # Build keyed frames (handles dist vs trans)
    cur = _keyed(cur_raw, dataset=dataset)
    prev = _keyed(prev_raw, dataset=dataset)

    _ensure_unique_keys(cur, context=f"CURRENT {cur_dir}")
    _ensure_unique_keys(prev, context=f"PREVIOUS {prev_dir}")

    cur_keys = set(cur["_key"])
    prev_keys = set(prev["_key"])
    both = cur_keys & prev_keys
    only_cur = cur_keys - prev_keys
    only_prev = prev_keys - cur_keys

    cur_active = dict(zip(cur["_key"], cur["_is_active"]))
    prev_active = dict(zip(prev["_key"], prev["_is_active"]))

    # ADDED
    reactivated = {k for k in both if (not prev_active[k]) and cur_active[k]}
    added_new = {k for k in only_cur if cur_active.get(k, False)}
    added_keys = reactivated | added_new

    # REMOVED
    removed_soft = {k for k in both if prev_active[k] and (not cur_active[k])}
    removed_missing = {k for k in only_prev if prev_active.get(k, False)}
    removed_keys = removed_soft | removed_missing

    # UPDATED (active in both, compare non-key cols)
    common_cols = sorted(set(cur.columns) & set(prev.columns))
    ignore_cols = {
        "_key",
        "_is_active",
        "_removal_dt",
        "_scope_id",
        "_floc",
        "_visit_no",
        "_scope_package_slot",
        "_scope_package_slot".lower(),
    }
    compare_cols = [c for c in common_cols if c not in ignore_cols]

    active_both = [k for k in both if prev_active[k] and cur_active[k]]
    updated_keys: set[str] = set()

    if compare_cols and active_both:
        c = cur[cur["_key"].isin(active_both)].sort_values("_key")
        p = prev[prev["_key"].isin(active_both)].sort_values("_key")

        c_sig = row_hashes(c[compare_cols])
        p_sig = row_hashes(p[compare_cols])

        tmp = pd.DataFrame({"_key": c["_key"].to_list(), "_sig_cur": c_sig.to_list()}).merge(
            pd.DataFrame({"_key": p["_key"].to_list(), "_sig_prev": p_sig.to_list()}),
            on="_key",
            how="inner",
        )
        updated_keys = set(tmp.loc[tmp["_sig_cur"] != tmp["_sig_prev"], "_key"].to_list())

    # Output frames
    added_df = cur[cur["_key"].isin(added_keys)].copy()
    added_df["_delta_type"] = "reactivated"
    added_df.loc[added_df["_key"].isin(added_new), "_delta_type"] = "added_new"

    removed_soft_df = cur[cur["_key"].isin(removed_soft)].copy()
    removed_soft_df["_delta_type"] = "removed_soft"

    removed_missing_df = prev[prev["_key"].isin(removed_missing)].copy()
    removed_missing_df["_delta_type"] = "removed_missing_in_current"

    removed_df = pd.concat([removed_soft_df, removed_missing_df], ignore_index=True)

    updated_df = cur[cur["_key"].isin(updated_keys)].copy()
    updated_df["_delta_type"] = "updated_active"

    # Write outputs (Silver HISTORY-only)
    out_dir = paths.local_dir(
        "silver",
        f"{dataset}_delta",
        "HISTORY",
        partitions={"vendor": vendor, **run_partitions},
        ensure=True,
    )

    added_path = write_dataset(added_df, out_dir, basename="added")
    removed_path = write_dataset(removed_df, out_dir, basename="removed")
    updated_path = write_dataset(updated_df, out_dir, basename="updated")

    mode = "transmission" if "transmission" in dataset.lower() else "distribution"
    metrics = {
        "delta_mode": f"keyed_{mode}",
        "delta_previous_found": True,
        "delta_previous_dir": str(prev_dir),
        "delta_current_dir": str(cur_dir),
        "added_rows": int(len(added_df)),
        "reactivated_rows": int((added_df["_delta_type"] == "reactivated").sum()) if len(added_df) else 0,
        "removed_rows": int(len(removed_df)),
        "removed_soft_rows": int(len(removed_soft_df)),
        "removed_missing_rows": int(len(removed_missing_df)),
        "updated_rows": int(len(updated_df)),
        "delta_out_dir": str(out_dir),
        "delta_added_file": str(added_path),
        "delta_removed_file": str(removed_path),
        "delta_updated_file": str(updated_path),
        "key_cols": ["_scope_id", "_floc", "_visit_no"] if mode == "transmission" else ["_scope_id", "_floc"],
        "distribution_blank_scope_filled_with": "COMP" if mode == "distribution" else None,
        "transmission_blank_scope_dropped": True if mode == "transmission" else None,
    }
    return metrics, prev_dir
