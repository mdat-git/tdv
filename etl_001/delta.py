from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from inspections_lakehouse.util.hashing import row_hashes
from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.dataset_io import write_dataset


KEY_COLS = ["SCOPE_ID", "FLOC"]
REMOVAL_COL = "SCOPE_REMOVAL_DATE"


def _read_dataset_file(dir_path: Path) -> pd.DataFrame:
    parquet = dir_path / "data.parquet"
    csv = dir_path / "data.csv"
    if parquet.exists():
        return pd.read_parquet(parquet)
    if csv.exists():
        return pd.read_csv(csv)
    raise FileNotFoundError(f"No data.parquet or data.csv found in: {dir_path}")


def _find_latest_prior_snapshot_dir(dataset: str, vendor: str, current_run_id: str) -> Optional[Path]:
    """
    Recursively find the most recent prior snapshot dir under:
      bronze/<dataset>/HISTORY/vendor=<vendor>/...
    Works with both old layouts (release=... folders) and new layouts.
    """
    base = paths.local_dir("bronze", dataset, "HISTORY", partitions={"vendor": vendor}, ensure=True)

    candidates: list[Path] = []
    for f in base.rglob("data.parquet"):
        candidates.append(f.parent)
    for f in base.rglob("data.csv"):
        candidates.append(f.parent)

    if not candidates:
        return None

    filtered = [d for d in candidates if f"run_id={current_run_id}" not in str(d)]
    if not filtered:
        return None

    return max(filtered, key=lambda d: d.stat().st_mtime)


def _require_cols(df: pd.DataFrame, cols: list[str], *, context: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}")


def _make_key(df: pd.DataFrame) -> pd.Series:
    # Normalize to strings for stable keys
    scope = df["SCOPE_ID"].astype("string").fillna("").str.strip()
    floc = df["FLOC"].astype("string").fillna("").str.strip()
    return scope + "|" + floc


def _is_active(df: pd.DataFrame) -> pd.Series:
    """
    Active = SCOPE_REMOVAL_DATE is null/empty.
    We coerce to datetime; anything parseable -> removed (inactive).
    """
    raw = df[REMOVAL_COL]
    # Treat empty strings as null
    raw2 = raw.astype("string").replace({"": None, "nan": None, "NaT": None})
    dt = pd.to_datetime(raw2, errors="coerce")
    return dt.isna()


def _check_unique_keys(df: pd.DataFrame, *, context: str) -> None:
    k = _make_key(df)
    dup = k.duplicated(keep=False)
    if dup.any():
        # Keep message short but actionable
        sample = k[dup].value_counts().head(10).to_dict()
        raise ValueError(
            f"Duplicate business keys detected in {context} for (SCOPE_ID,FLOC). "
            f"Sample duplicates (key:count): {sample}"
        )


def compute_key_delta(
    *,
    dataset: str,
    vendor: str,
    run_partitions: dict[str, str],
) -> Tuple[dict, Optional[Path]]:
    """
    Key-based delta using (SCOPE_ID, FLOC) with soft-removal via SCOPE_REMOVAL_DATE.

    Writes HISTORY-only to:
      silver/<dataset>_delta/HISTORY/vendor=.../run_date=.../run_id=.../{added|removed|updated}.parquet|csv

    Returns:
      (metrics, prev_snapshot_dir_or_none)
    """
    run_id = run_partitions["run_id"]

    cur_dir = paths.local_dir(
        "bronze", dataset, "HISTORY",
        partitions={"vendor": vendor, **run_partitions},
        ensure=False,
    )
    prev_dir = _find_latest_prior_snapshot_dir(dataset, vendor, run_id)

    # If no prior snapshot, we skip delta
    if prev_dir is None:
        metrics = {
            "delta_mode": "key_scope_floc",
            "delta_previous_found": False,
            "delta_current_dir": str(cur_dir),
            "added_rows": 0,
            "removed_rows": 0,
            "updated_rows": 0,
            "reactivated_rows": 0,
            "note": "No prior snapshot found; delta skipped.",
        }
        return metrics, None

    cur = _read_dataset_file(cur_dir)
    prev = _read_dataset_file(prev_dir)

    _require_cols(cur, KEY_COLS + [REMOVAL_COL], context=f"CURRENT snapshot {cur_dir}")
    _require_cols(prev, KEY_COLS + [REMOVAL_COL], context=f"PREVIOUS snapshot {prev_dir}")

    _check_unique_keys(cur, context=f"CURRENT snapshot {cur_dir}")
    _check_unique_keys(prev, context=f"PREVIOUS snapshot {prev_dir}")

    cur_key = _make_key(cur)
    prev_key = _make_key(prev)

    cur_active = _is_active(cur)
    prev_active = _is_active(prev)

    cur2 = cur.copy()
    prev2 = prev.copy()
    cur2["_key"] = cur_key
    prev2["_key"] = prev_key
    cur2["_is_active"] = cur_active
    prev2["_is_active"] = prev_active

    cur_keys = set(cur2["_key"].tolist())
    prev_keys = set(prev2["_key"].tolist())
    keys_both = cur_keys & prev_keys
    keys_only_cur = cur_keys - prev_keys
    keys_only_prev = prev_keys - cur_keys

    # Build quick lookup for active/inactive by key
    cur_active_map = dict(zip(cur2["_key"], cur2["_is_active"]))
    prev_active_map = dict(zip(prev2["_key"], prev2["_is_active"]))

    # Added vs Reactivated (becomes active now)
    reactivated_keys = {k for k in keys_both if (not prev_active_map[k]) and cur_active_map[k]}
    new_added_keys = {k for k in keys_only_cur if cur_active_map[k]}
    added_keys = reactivated_keys | new_added_keys

    # Removed (was active before, now not active OR missing)
    removed_soft_keys = {k for k in keys_both if prev_active_map[k] and (not cur_active_map[k])}
    removed_missing_keys = {k for k in keys_only_prev if prev_active_map[k]}
    removed_keys = removed_soft_keys | removed_missing_keys

    # Updated (active in both, but non-key attributes changed)
    # Compare only common columns, excluding keys and removal date and our helper cols
    common_cols = sorted(set(cur.columns) & set(prev.columns))
    compare_cols = [c for c in common_cols if c not in (KEY_COLS + [REMOVAL_COL])]
    # Only consider updates for keys that are active in both
    active_both_keys = [k for k in keys_both if prev_active_map[k] and cur_active_map[k]]

    updated_keys: set[str] = set()
    if compare_cols and active_both_keys:
        cur_active_df = cur2[cur2["_key"].isin(active_both_keys)].copy()
        prev_active_df = prev2[prev2["_key"].isin(active_both_keys)].copy()

        # Ensure stable row ordering by key
        cur_active_df = cur_active_df.sort_values("_key")
        prev_active_df = prev_active_df.sort_values("_key")

        cur_sig = row_hashes(cur_active_df[compare_cols])
        prev_sig = row_hashes(prev_active_df[compare_cols])

        cur_active_df["_sig"] = cur_sig.values
        prev_active_df["_sig"] = prev_sig.values

        merged = prev_active_df[["_key", "_sig"]].merge(
            cur_active_df[["_key", "_sig"]],
            on="_key",
            how="inner",
            suffixes=("_prev", "_cur"),
        )
        updated_keys = set(merged.loc[merged["_sig_prev"] != merged["_sig_cur"], "_key"].tolist())

    # Build delta frames
    # Added rows come from current snapshot
    added_df = cur2[cur2["_key"].isin(added_keys)].copy()
    added_df["_delta_type"] = "reactivated"
    added_df.loc[added_df["_key"].isin(new_added_keys), "_delta_type"] = "added_new"

    # Removed rows:
    # - soft removal -> take the current row (shows removal date)
    # - missing -> take the previous row (last known)
    removed_soft_df = cur2[cur2["_key"].isin(removed_soft_keys)].copy()
    removed_soft_df["_delta_type"] = "removed_soft"

    removed_missing_df = prev2[prev2["_key"].isin(removed_missing_keys)].copy()
    removed_missing_df["_delta_type"] = "removed_missing_in_current"

    removed_df = pd.concat([removed_soft_df, removed_missing_df], ignore_index=True)

    # Updated rows come from current snapshot (active)
    updated_df = cur2[cur2["_key"].isin(updated_keys)].copy()
    updated_df["_delta_type"] = "updated_active"

    # Write outputs (HISTORY only)
    delta_dataset = f"{dataset}_delta"
    out_dir = paths.local_dir(
        "silver",
        delta_dataset,
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
        "key_cols": KEY_COLS,
        "removal_col": REMOVAL_COL,
    }
    return metrics, prev_dir
