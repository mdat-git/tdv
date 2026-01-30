# src/inspections_lakehouse/etl/etl_010_vendor_invoice_intake/pipeline.py
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd

from inspections_lakehouse.util.paths import paths, Layer
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.etl.etl_010_vendor_invoice_intake.silverize import (
    parse_ariba_email_html,
    parse_invoice_excel,
    silverize_invoice_header,
    silverize_invoice_lines,
)


PIPELINE = "etl_010_vendor_invoice_intake"

# Datasets
BRONZE_HEADER_KV = "vendor_invoice_header_kv_raw"
BRONZE_LINE_RAW = "vendor_invoice_line_raw"
SILVER_HEADER = "vendor_invoice_header"
SILVER_LINE = "vendor_invoice_line"

# Filename conventions
ATT_RE = re.compile(r"^att__(?P<message_id>[^_]+)__.+\.(?P<ext>xlsx|xlsm|xls)$", re.IGNORECASE)


def _default_attachments_dir() -> Path:
    return paths.uploads_root / "stg" / "invoices" / "attachments"


def _default_email_html_dir() -> Path:
    return paths.uploads_root / "stg" / "invoices" / "email_html"


def _find_matching_html(message_id: str, html_dir: Path) -> Optional[Path]:
    # Spec: email__{messageId}_.html
    p = html_dir / f"email__{message_id}_.html"
    if p.exists():
        return p
    return None


def _list_attachments(attachments_dir: Path) -> list[Path]:
    if not attachments_dir.exists():
        raise FileNotFoundError(f"Attachments dir not found: {attachments_dir}")

    files = []
    for p in sorted(attachments_dir.iterdir()):
        if p.is_file() and ATT_RE.match(p.name):
            files.append(p)
    return files


def _archive_files(*, attachment_path: Path, html_path: Path, run) -> Dict[str, Any]:
    """Copy the raw files to uploads provenance under category vendor_invoices/<run partitions>."""
    base = paths.uploads_dir("vendor_invoices", partitions={"run_date": run.run_date, "run_id": run.run_id}, ensure=True)
    att_out_dir = base / "attachments"
    html_out_dir = base / "email_html"
    att_out_dir.mkdir(parents=True, exist_ok=True)
    html_out_dir.mkdir(parents=True, exist_ok=True)

    att_out = att_out_dir / attachment_path.name
    html_out = html_out_dir / html_path.name

    shutil.copy2(attachment_path, att_out)
    shutil.copy2(html_path, html_out)

    return {"attachment_saved_to": str(att_out), "email_html_saved_to": str(html_out)}


def _read_existing_current(layer: Layer, dataset: str) -> pd.DataFrame:
    curr_dir = paths.local_dir(layer=layer, dataset=dataset, version="CURRENT", partitions={}, ensure=False)
    parquet = curr_dir / "data.parquet"
    csv = curr_dir / "data.csv"

    if parquet.exists():
        try:
            return pd.read_parquet(parquet)
        except Exception:
            # fall back to csv if parquet read fails
            if csv.exists():
                return pd.read_csv(csv, dtype=str, keep_default_na=False)
            return pd.DataFrame()

    if csv.exists():
        return pd.read_csv(csv, dtype=str, keep_default_na=False)

    return pd.DataFrame()


def _write_history(df: pd.DataFrame, *, layer: Layer, dataset: str, run) -> Path:
    out_hist = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="HISTORY",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    return write_dataset(df, Path(out_hist), basename="data")


def _write_current_accumulating(df_new: pd.DataFrame, *, layer: Layer, dataset: str, dedupe_keys: list[str]) -> Path:
    curr = _read_existing_current(layer, dataset)
    if not curr.empty:
        combined = pd.concat([curr, df_new], ignore_index=True)
    else:
        combined = df_new

    # Dedupe if keys exist
    if dedupe_keys and all(k in combined.columns for k in dedupe_keys):
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")

    out_curr = paths.local_dir(layer=layer, dataset=dataset, version="CURRENT", partitions={}, ensure=True)
    return write_dataset(combined, Path(out_curr), basename="data")


def run_pipeline(
    *,
    run,
    attachments_dir: Optional[Path],
    email_html_dir: Optional[Path],
    source_system: str,
    max_files: Optional[int] = None,
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "processed": 0,
        "skipped_orphan": 0,
        "skipped_bad_format": 0,
        "rows": {"bronze_line": 0, "silver_line": 0},
    }

    att_dir = attachments_dir or _default_attachments_dir()
    html_dir = email_html_dir or _default_email_html_dir()

    files = _list_attachments(att_dir)
    if max_files is not None:
        files = files[: max_files]

    all_bronze_kv: list[pd.DataFrame] = []
    all_bronze_lines: list[pd.DataFrame] = []
    all_silver_header: list[pd.DataFrame] = []
    all_silver_lines: list[pd.DataFrame] = []

    per_file: list[dict[str, Any]] = []

    for att_path in files:
        m = ATT_RE.match(att_path.name)
        if not m:
            continue

        message_id = m.group("message_id")
        html_path = _find_matching_html(message_id, html_dir)

        if html_path is None:
            metrics["skipped_orphan"] += 1
            per_file.append({"attachment": att_path.name, "message_id": message_id, "status": "SKIPPED_NO_HTML"})
            continue

        try:
            html_fields = parse_ariba_email_html(html_path)
            excel_header_kv, excel_lines = parse_invoice_excel(att_path)

            # provenance archive
            arch = _archive_files(attachment_path=att_path, html_path=html_path, run=run)

            # Bronze tables
            df_kv = pd.DataFrame([{
                **excel_header_kv,
                **{f"ariba_{k}": v for k, v in html_fields.items()},
                "message_id": message_id,
                "attachment_file": att_path.name,
                "email_html_file": html_path.name,
                "run_date": run.run_date,
                "run_id": run.run_id,
                "source_system": source_system,
            }])

            df_lines_raw = excel_lines.copy()
            df_lines_raw["message_id"] = message_id
            df_lines_raw["attachment_file"] = att_path.name
            df_lines_raw["email_html_file"] = html_path.name
            df_lines_raw["run_date"] = run.run_date
            df_lines_raw["run_id"] = run.run_id
            df_lines_raw["source_system"] = source_system

            # Silver tables
            df_head = silverize_invoice_header(
                excel_header_kv=excel_header_kv,
                html_fields=html_fields,
                message_id=message_id,
                attachment_file=att_path.name,
                email_html_file=html_path.name,
                run=run,
                source_system=source_system,
                archive_paths=arch,
            )
            df_line = silverize_invoice_lines(
                df_lines_raw=excel_lines,
                header_row=df_head.iloc[0].to_dict(),
                message_id=message_id,
                attachment_file=att_path.name,
                email_html_file=html_path.name,
                run=run,
                source_system=source_system,
            )

            all_bronze_kv.append(df_kv)
            all_bronze_lines.append(df_lines_raw)
            all_silver_header.append(df_head)
            all_silver_lines.append(df_line)

            metrics["processed"] += 1
            metrics["rows"]["bronze_line"] += int(len(df_lines_raw))
            metrics["rows"]["silver_line"] += int(len(df_line))

            per_file.append({
                "attachment": att_path.name,
                "message_id": message_id,
                "status": "OK",
                "lines": int(len(df_line)),
                "supplier": df_head.iloc[0].get("supplier"),
            })

        except Exception as e:
            metrics["skipped_bad_format"] += 1
            per_file.append({"attachment": att_path.name, "message_id": message_id, "status": "SKIPPED_BAD_FORMAT", "error": str(e)})
            continue

    metrics["files"] = per_file

    # Nothing to write
    if not all_silver_header:
        metrics["write"] = "no_data"
        return metrics

    df_bronze_kv_all = pd.concat(all_bronze_kv, ignore_index=True) if all_bronze_kv else pd.DataFrame()
    df_bronze_line_all = pd.concat(all_bronze_lines, ignore_index=True) if all_bronze_lines else pd.DataFrame()
    df_silver_head_all = pd.concat(all_silver_header, ignore_index=True)
    df_silver_line_all = pd.concat(all_silver_lines, ignore_index=True)

    # HISTORY (this run only)
    metrics["write_history"] = {
        "bronze_header_kv": str(_write_history(df_bronze_kv_all, layer="bronze", dataset=BRONZE_HEADER_KV, run=run)),
        "bronze_line": str(_write_history(df_bronze_line_all, layer="bronze", dataset=BRONZE_LINE_RAW, run=run)),
        "silver_header": str(_write_history(df_silver_head_all, layer="silver", dataset=SILVER_HEADER, run=run)),
        "silver_line": str(_write_history(df_silver_line_all, layer="silver", dataset=SILVER_LINE, run=run)),
    }

    # CURRENT (accumulate + dedupe)
    metrics["write_current"] = {
        "bronze_header_kv": str(_write_current_accumulating(df_bronze_kv_all, layer="bronze", dataset=BRONZE_HEADER_KV, dedupe_keys=["message_id"])),
        "bronze_line": str(_write_current_accumulating(df_bronze_line_all, layer="bronze", dataset=BRONZE_LINE_RAW, dedupe_keys=["message_id", "_row_index"] if "_row_index" in df_bronze_line_all.columns else ["message_id"])),
        "silver_header": str(_write_current_accumulating(df_silver_head_all, layer="silver", dataset=SILVER_HEADER, dedupe_keys=["message_id"])),
        "silver_line": str(_write_current_accumulating(df_silver_line_all, layer="silver", dataset=SILVER_LINE, dedupe_keys=["message_id", "line_number"])),
    }

    return metrics
