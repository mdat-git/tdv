"""
Reusable helpers extracted from your ingestion script.

These are dataset-agnostic (or easily parameterized) and can be moved into something like:
  ./platform/io_utils.py
  ./platform/metadata.py
  ./platform/util_logging.py
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# ----------------------------
# Time
# ----------------------------

def utc_now() -> datetime:
    """Current UTC timestamp (timezone-aware)."""
    return datetime.now(timezone.utc)


# ----------------------------
# Files / IO
# ----------------------------

def sha256_file(path: Path) -> str:
    """Compute a SHA-256 hash of a file's bytes (chunked for large files)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_csv(df: pd.DataFrame, out_path: Path) -> None:
    """
    Write a CSV atomically: write to a temp file, then replace.
    Prevents partial writes if a job crashes mid-write.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(out_path)


def append_csv_row(row: dict, out_path: Path) -> None:
    """
    Append a single dict row to a CSV (creates the CSV with header if missing).
    Handy for simple "log tables" in file-based lakehouse setups.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    if out_path.exists():
        df.to_csv(out_path, mode="a", header=False, index=False)
    else:
        df.to_csv(out_path, mode="w", header=True, index=False)


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    """Read CSV to DataFrame if file exists, else return empty DataFrame."""
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleanup: trim whitespace from column names."""
    out = df.copy()
    out.columns = [c.strip() for c in out.columns]
    return out


# ----------------------------
# Row-level metadata attachment (generic)
# ----------------------------

def attach_row_metadata(
    df: pd.DataFrame,
    *,
    pipeline_name: str,
    run_id: str,
    file_id: Optional[str],
    ingested_at_utc: datetime,
    report_date,  # accepts datetime/date/string; stored as ISO string
    source_system: str,
    source_file_name: Optional[str] = None,
    source_file_path: Optional[str] = None,
    **extra_fields
) -> pd.DataFrame:
    """
    Attach standard lineage/metadata columns to every row.

    `extra_fields` lets each pipeline add dataset-specific fields (e.g., dataset_type).
    Example:
        attach_row_metadata(df, ..., dataset_type="distribution")
    """
    out = df.copy()

    out["pipeline_name"] = pipeline_name
    out["run_id"] = run_id
    out["file_id"] = file_id

    out["ingested_at_utc"] = ingested_at_utc.isoformat()
    out["report_date"] = report_date.isoformat() if hasattr(report_date, "isoformat") else str(report_date)

    out["source_system"] = source_system
    out["source_file_name"] = source_file_name
    out["source_file_path"] = source_file_path

    for k, v in extra_fields.items():
        out[k] = v

    return out


# ----------------------------
# File-based UTIL logging (generic)
# ----------------------------

def util_file_already_loaded(ingest_files_path: Path, file_hash: str) -> bool:
    """
    Check whether this file_hash has already been successfully LOADED.
    """
    df = read_csv_if_exists(ingest_files_path)
    if df.empty:
        return False
    return ((df.get("file_hash_sha256") == file_hash) & (df.get("status") == "LOADED")).any()


def util_upsert_file_row(ingest_files_path: Path, file_hash: str, updates: dict) -> None:
    """
    Upsert into a CSV "table" keyed by file_hash_sha256.
    - If exists: update the row
    - Else: append a new row
    """
    df = read_csv_if_exists(ingest_files_path)
    if df.empty:
        atomic_write_csv(pd.DataFrame([updates]), ingest_files_path)
        return

    if "file_hash_sha256" not in df.columns:
        raise ValueError("INGEST_FILES missing 'file_hash_sha256' column")

    mask = df["file_hash_sha256"] == file_hash
    if mask.any():
        for k, v in updates.items():
            if k not in df.columns:
                df[k] = pd.NA
            df.loc[mask, k] = v
        atomic_write_csv(df, ingest_files_path)
    else:
        append_csv_row(updates, ingest_files_path)


def util_log_run_start(ingest_runs_path: Path, run_row: dict) -> None:
    """Append a run-start record to INGEST_RUNS."""
    append_csv_row(run_row, ingest_runs_path)


def util_log_run_end(
    ingest_runs_path: Path,
    run_id: str,
    status: str,
    ended_at_utc: datetime,
    error_message: Optional[str] = None
) -> None:
    """
    Update the run row for run_id in INGEST_RUNS with final status + end timestamp.
    """
    df = read_csv_if_exists(ingest_runs_path)
    if df.empty or "run_id" not in df.columns:
        return

    mask = df["run_id"] == run_id
    if not mask.any():
        return

    for c in ["status", "ended_at_utc", "error_message"]:
        if c not in df.columns:
            df[c] = pd.NA

    df.loc[mask, "status"] = status
    df.loc[mask, "ended_at_utc"] = ended_at_utc.isoformat()
    if error_message:
        df.loc[mask, "error_message"] = error_message

    atomic_write_csv(df, ingest_runs_path)
