from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.hashing import row_hashes
from inspections_lakehouse.util.scope_keys import add_business_key, REMOVAL_COL


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
    Find latest prior snapshot for this vendor (works with old layouts containing extra folders like release=...).
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


def _is_active(removal_series: pd.Series) -> pd.Series:
    """
    Active if SCOPE_REMOVAL_DATE is null/empty/unparseable as datetime.
    """
    s = removal_series.astype("string").replace({"": None, "nan": None, "NaT": None})
    dt = pd.to_datetime(s, errors="coerce")
    return dt.isna()


def _ensure_unique_keys(df: pd.DataFrame, context: str) -> None:
    if df["_key"].duplicated().any():
        sample = df.loc[df["_key"].duplicated(keep=False), "_key"].value_counts().head(10).to_dict()
        raise ValueError(
            f"Duplicate (SCOPE_ID,FLOC) keys in {context}. "
            f"Likely collision (e.g., blank SCOPE_ID -> COMP). Sample: {sample}"
        )


def compute_key_delta(
    *,
    dataset: str,
    vendor: str,
    run_partitions: dict[str, str],
) -> Tuple[dict, Optional[Path]]:
    """
    Key-based delta using:
      key = normalized SCOPE_ID (blank->COMP) + FLOC
      active = SCOPE_REMOVAL_DATE is null

    Writes HISTORY-only:
      silver/<dataset>_delta/HISTORY/vendor=.../run_date=.../run_id=.../{added,removed,updated}.parquet|csv
    """
    run_id = run_partitions["run_id"]

    cur_dir = paths.local_dir("bronze", dataset, "HISTORY", partitions={"vendor": vendor, **run_partitions}, ensure=False)
    prev_dir = _latest_prior_history_dir(dataset, vendor, run_id)

    if prev_dir is None:
        return (
            {
                "delta_mode": "key_scope_floc",
                "delta_previous_found": False,
                "delta_current_dir": str(cur_dir),
                "added_rows": 0,
                "removed_rows": 0,
                "updated_rows": 0,
                "note": "No prior snapshot found; delta skipped.",
            },
            None,
        )

    cur = _read_snapshot(cur_dir)
    prev = _read_snapshot(prev_dir)

    # Build business keys (blank SCOPE_ID => COMP) and active flags
    cur = add_business_key(cur, key_col="_key")
    prev = add_business_key(prev, key_col="_key")

    if REMOVAL_COL not in cur.columns or REMOVAL_COL not in prev.columns:
        raise ValueError(f"Missing required column '{REMOVAL_COL}' in one or both snapshots.")

    cur["_is_active"] = _is_active(cur[REMOVAL_COL])
    prev["_is_active"] = _is_active(prev[REMOVAL_COL])

    _ensure_unique_keys(cur, context=f"CURRENT {cur_dir}")
    _ensure_unique_keys(prev, context=f"PREVIOUS {prev_dir}")

    cur_keys = set(cur["_key"])
    prev_keys = set(prev["_key"])
    both = cur_keys & prev_keys
    only_cur = cur_keys - prev_keys
    only_prev = prev_keys - cur_keys

    cur_active = dict(zip(cur["_key"], cur["_is_active"]))
    prev_active = dict(zip(prev["_key"], prev["_is_active"]))

    # ADDED: new active keys OR reactivated keys
    reactivated = {k for k in both if (not prev_active[k]) and cur_active[k]}
    added_new = {k for k in only_cur if cur_active.get(k, False)}
    added_keys = reactivated | added_new

    # REMOVED:
    # - soft: was active, now inactive (removal date filled)
    removed_soft = {k for k in both if prev_active[k] and (not cur_active[k])}
    # - missing: was active, now missing entirely (rare; log it)
    removed_missing = {k for k in only_prev if prev_active.get(k, False)}
    removed_keys = removed_soft | removed_missing

    # UPDATED: active in both, but non-key attributes changed
    common_cols = sorted(set(cur.columns) & set(prev.columns))
    ignore_cols = {"_key", "_is_active", REMOVAL_COL, "SCOPE_ID", "FLOC"}
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

    # Build output frames
    added_df = cur[cur["_key"].isin(added_keys)].copy()
    added_df["_delta_type"] = "reactivated"
    added_df.loc[added_df["_key"].isin(added_new), "_delta_type"] = "added_new"

    # removed rows: for soft removals use CURRENT (shows removal date); for missing use PREVIOUS (last known)
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

    metrics = {
        "delta_mode": "key_scope_floc",
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
        "key_cols": ["SCOPE_ID", "FLOC"],
        "scope_blank_filled_with": "COMP",
        "removal_col": REMOVAL_COL,
    }
    return metrics, prev_dir
