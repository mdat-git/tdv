from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.vendors import VALID_VENDORS

PIPELINE = "etl_004_gold_scope_assignment_current"

# ----------------------------
# Edit these ONLY if your dataset names differ
# ----------------------------
DRONE_DIST_DATASET = "scope_release_distribution"
DRONE_TRANS_DATASET = "scope_release_transmission"

HELO_DIST_DATASET = "helo_scope_distribution"
HELO_TRANS_DATASET = "helo_scope_transmission"

EVENTS_DATASET = "scope_events_line"

GOLD_DIST_DATASET = "scope_assignment_current_distribution"
GOLD_TRANS_DATASET = "scope_assignment_current_transmission"


# ----------------------------
# Read helpers
# ----------------------------
def _read_data_file(dir_path: Path) -> pd.DataFrame:
    p = dir_path / "data.parquet"
    c = dir_path / "data.csv"
    if p.exists():
        return pd.read_parquet(p)
    if c.exists():
        return pd.read_csv(c)
    raise FileNotFoundError(f"No data.parquet or data.csv found in: {dir_path}")


def _maybe_read_current_silver(dataset_line: str, vendor: str) -> Optional[pd.DataFrame]:
    """
    Reads: silver/<dataset_line>/CURRENT/vendor=.../data.parquet|csv
    Returns None if not found.
    """
    d = paths.local_dir("silver", dataset_line, "CURRENT", partitions={"vendor": vendor}, ensure=False)
    try:
        df = _read_data_file(d)
        return df
    except FileNotFoundError:
        return None


def _load_all_events_history(vendor: str) -> pd.DataFrame:
    """
    Walk:
      silver/scope_events_line/HISTORY/vendor=.../**/data.parquet|csv
    """
    base = paths.local_dir("silver", EVENTS_DATASET, "HISTORY", partitions={"vendor": vendor}, ensure=False)

    files: list[Path] = []
    files.extend(base.rglob("data.parquet"))
    files.extend(base.rglob("data.csv"))

    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for f in files:
        if f.suffix.lower() == ".parquet":
            frames.append(pd.read_parquet(f))
        else:
            frames.append(pd.read_csv(f))

    out = pd.concat(frames, ignore_index=True)
    return out


# ----------------------------
# Event collapse
# ----------------------------
_EVENT_PRIORITY = {"move_to_helo": 2, "removal": 1}


def _collapse_latest_events(df_events: pd.DataFrame) -> pd.DataFrame:
    """
    Input must include:
      vendor, scope_floc_key, event_type, event_effective_date, run_date, run_id
    Output: one row per (vendor, scope_floc_key) = latest event for joining.
    """
    if df_events.empty:
        return df_events

    e = df_events.copy()

    # normalize
    for c in ["vendor", "scope_floc_key", "event_type", "run_date", "run_id"]:
        if c in e.columns:
            e[c] = e[c].astype("string")

    if "event_effective_date" in e.columns:
        e["event_effective_date"] = pd.to_datetime(e["event_effective_date"], errors="coerce")
    else:
        e["event_effective_date"] = pd.NaT

    e["_event_priority"] = e["event_type"].map(_EVENT_PRIORITY).fillna(0).astype("int64")

    # Sort so the "best" row per key is first:
    # 1) newest effective date
    # 2) newest run_date (string ISO sorts OK if YYYY-MM-DD)
    # 3) prefer move_to_helo on ties
    # 4) run_id tie-break (deterministic only)
    e = e.sort_values(
        by=["event_effective_date", "run_date", "_event_priority", "run_id"],
        ascending=[False, False, False, False],
        na_position="last",
    )

    latest = e.drop_duplicates(subset=["vendor", "scope_floc_key"], keep="first").copy()
    latest = latest.drop(columns=["_event_priority"], errors="ignore")

    # Keep only the join + audit columns we care about (but tolerate extras)
    keep = [
        "vendor",
        "scope_floc_key",
        "event_type",
        "event_effective_date",
        "run_date",
        "run_id",
        "source_file_saved",
        "source_sheet",
        "visit_no",
        "scope_id",
        "floc",
    ]
    cols = [c for c in keep if c in latest.columns]
    return latest[cols].copy()


# ----------------------------
# Assignment build (per asset class)
# ----------------------------
def _prep_assignment_frame(
    df: pd.DataFrame,
    *,
    method: str,                 # "DRONE" | "HELO"
    vendor: str,
    asset_class: str,            # "Distribution" | "Transmission"
    source_dataset: str,
) -> pd.DataFrame:
    """
    Normalizes minimum columns needed for Gold:
      vendor, floc, scope_id, scope_floc_key, base_is_active, assignment_method, asset_class, + source pointers
    """
    out = df.copy()

    # make sure required columns exist
    for c in ["floc", "scope_id", "scope_floc_key"]:
        if c not in out.columns:
            raise KeyError(f"Missing required column '{c}' in {source_dataset} CURRENT for vendor={vendor}")

    out["vendor"] = out.get("vendor", vendor)
    out["vendor"] = out["vendor"].astype("string")
    out["assignment_method"] = method
    out["asset_class"] = asset_class
    out["source_dataset"] = source_dataset

    # base is_active:
    if method == "HELO":
        out["base_is_active"] = True
    else:
        # DRONE uses ETL001 silver line is_active
        if "is_active" in out.columns:
            out["base_is_active"] = out["is_active"].astype("boolean")
        else:
            out["base_is_active"] = True  # fallback

    # ensure strings for key columns (parquet safety)
    out["floc"] = out["floc"].astype("string")
    out["scope_id"] = out["scope_id"].astype("string")
    out["scope_floc_key"] = out["scope_floc_key"].astype("string")

    return out


def _choose_preferred_assignment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce to ONE row per (vendor, scope_floc_key):
      Prefer HELO over DRONE.
    """
    if df.empty:
        return df

    d = df.copy()
    d["_method_pri"] = d["assignment_method"].map({"HELO": 2, "DRONE": 1}).fillna(0).astype("int64")

    # If both exist, pick HELO; otherwise pick the only row
    d = d.sort_values(by=["_method_pri"], ascending=False)
    d = d.drop_duplicates(subset=["vendor", "scope_floc_key"], keep="first").drop(columns=["_method_pri"])
    return d


def _apply_events_to_assignments(assignments: pd.DataFrame, latest_events: pd.DataFrame) -> pd.DataFrame:
    """
    Produces final:
      is_active_assignment, assignment_status, latest_event_type/date
    Rules:
      - move_to_helo overrides removal
      - removal => inactive
      - move_to_helo => helo active if helo row exists, else inactive + flag
      - otherwise => base_is_active
    """
    if assignments.empty:
        return assignments

    a = assignments.copy()

    # join latest event per key
    if latest_events is not None and not latest_events.empty:
        e = latest_events.copy()
        # only join columns we need
        e_keep = [c for c in ["vendor", "scope_floc_key", "event_type", "event_effective_date"] if c in e.columns]
        e = e[e_keep].copy()

        a = a.merge(e, on=["vendor", "scope_floc_key"], how="left")
    else:
        a["event_type"] = pd.NA
        a["event_effective_date"] = pd.NaT

    a["latest_event_type"] = a["event_type"].astype("string")
    a["latest_event_effective_date"] = pd.to_datetime(a["event_effective_date"], errors="coerce")

    # defaults
    a["is_active_assignment"] = a["base_is_active"].astype("boolean")
    a["assignment_status"] = pd.NA

    # removal
    is_removal = a["latest_event_type"].fillna("") == "removal"
    a.loc[is_removal, "is_active_assignment"] = False
    a.loc[is_removal, "assignment_status"] = "REMOVED"

    # move_to_helo
    is_move = a["latest_event_type"].fillna("") == "move_to_helo"
    # if the chosen assignment method is HELO, it's active; if DRONE, it's inactive
    a.loc[is_move & (a["assignment_method"] == "HELO"), "is_active_assignment"] = True
    a.loc[is_move & (a["assignment_method"] == "HELO"), "assignment_status"] = "ACTIVE_HELO"

    a.loc[is_move & (a["assignment_method"] == "DRONE"), "is_active_assignment"] = False
    a.loc[is_move & (a["assignment_method"] == "DRONE"), "assignment_status"] = "MOVED_TO_HELO"

    # no events
    no_evt = a["latest_event_type"].isna() | (a["latest_event_type"].fillna("") == "")
    a.loc[no_evt & (a["assignment_method"] == "HELO") & (a["is_active_assignment"]), "assignment_status"] = "ACTIVE_HELO"
    a.loc[no_evt & (a["assignment_method"] == "DRONE") & (a["is_active_assignment"]), "assignment_status"] = "ACTIVE_DRONE"
    a.loc[no_evt & (~a["is_active_assignment"]), "assignment_status"] = "INACTIVE"

    return a


# ----------------------------
# Main pipeline
# ----------------------------
def run_pipeline(run: RunLog, *, vendors: Optional[list[str]] = None) -> None:
    vendor_list = vendors or list(VALID_VENDORS)

    # 1) Load all events (HISTORY) across vendors, then collapse to latest per key
    events_all: list[pd.DataFrame] = []
    for v in vendor_list:
        ev = _load_all_events_history(v)
        if not ev.empty:
            events_all.append(ev)

    if events_all:
        events_df = pd.concat(events_all, ignore_index=True)
        run.metrics["events_history_rows_total"] = int(len(events_df))
        latest_events = _collapse_latest_events(events_df)
        run.metrics["events_latest_rows"] = int(len(latest_events))
    else:
        events_df = pd.DataFrame()
        latest_events = pd.DataFrame()
        run.metrics["events_history_rows_total"] = 0
        run.metrics["events_latest_rows"] = 0

    # 2) Build and write Gold for each asset class separately (Distribution / Transmission)
    dist_gold = _build_gold_for_asset_class(
        run=run,
        vendor_list=vendor_list,
        latest_events=latest_events,
        asset_class="Distribution",
        drone_dataset=DRONE_DIST_DATASET,
        helo_dataset=HELO_DIST_DATASET,
        gold_dataset=GOLD_DIST_DATASET,
    )

    trans_gold = _build_gold_for_asset_class(
        run=run,
        vendor_list=vendor_list,
        latest_events=latest_events,
        asset_class="Transmission",
        drone_dataset=DRONE_TRANS_DATASET,
        helo_dataset=HELO_TRANS_DATASET,
        gold_dataset=GOLD_TRANS_DATASET,
    )

    run.metrics["gold_distribution_rows"] = int(len(dist_gold))
    run.metrics["gold_transmission_rows"] = int(len(trans_gold))


def _build_gold_for_asset_class(
    *,
    run: RunLog,
    vendor_list: list[str],
    latest_events: pd.DataFrame,
    asset_class: str,
    drone_dataset: str,
    helo_dataset: str,
    gold_dataset: str,
) -> pd.DataFrame:
    """
    For a given asset class:
      - load ETL001 CURRENT for all vendors (DRONE)
      - load ETL003 CURRENT for all vendors (HELO)
      - union, prefer HELO
      - apply latest events
      - write GOLD HISTORY + CURRENT
    """
    # Load drone + helo
    frames: list[pd.DataFrame] = []

    for v in vendor_list:
        drone = _maybe_read_current_silver(f"{drone_dataset}_line", v)
        if drone is not None and not drone.empty:
            frames.append(_prep_assignment_frame(drone, method="DRONE", vendor=v, asset_class=asset_class, source_dataset=drone_dataset))

        helo = _maybe_read_current_silver(f"{helo_dataset}_line", v)
        if helo is not None and not helo.empty:
            frames.append(_prep_assignment_frame(helo, method="HELO", vendor=v, asset_class=asset_class, source_dataset=helo_dataset))

    if not frames:
        out = pd.DataFrame()
    else:
        all_assign = pd.concat(frames, ignore_index=True)

        # Reduce to 1 row per key preferring HELO
        preferred = _choose_preferred_assignment(all_assign)

        # Apply events (removal/move_to_helo) as override
        out = _apply_events_to_assignments(preferred, latest_events)

    # Add run metadata
    if not out.empty:
        out["gold_run_date"] = run.partitions["run_date"]
        out["gold_run_id"] = run.partitions["run_id"]

    # Write GOLD
    hist_dir = paths.local_dir("gold", gold_dataset, "HISTORY", partitions=run.partitions, ensure=True)
    hist_file = write_dataset(out, hist_dir, basename="data")

    curr_dir = paths.local_dir("gold", gold_dataset, "CURRENT", partitions=None, ensure=True)
    curr_file = write_dataset(out, curr_dir, basename="data")

    run.metrics.update(
        {
            f"{gold_dataset}__rows": int(len(out)),
            f"{gold_dataset}__history_file": str(hist_file),
            f"{gold_dataset}__current_file": str(curr_file),
        }
    )

    return out
