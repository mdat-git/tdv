# src/inspections_lakehouse/etl/etl_010_vendor_invoices_intake/pipeline.py
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from inspections_lakehouse.util.paths import paths, Layer
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.etl.etl_010_vendor_invoices_intake.silverize import (
    parse_ariba_email_html,
    read_invoice_excel_first_sheet,
    canonicalize_invoice_lines,
)

PIPELINE = "etl_010_vendor_invoices_intake"

ATT_RE = re.compile(r"^att__(?P<message_id>[^_]+)__(?P<invoice_name>.+)\.xlsx$", re.IGNORECASE)


def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")


def _write_whole_table(df: pd.DataFrame, *, layer: Layer, dataset: str, run) -> Dict[str, Any]:
    out_hist = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="HISTORY",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    write_dataset(df, Path(out_hist), basename="data")

    out_curr = paths.local_dir(layer=layer, dataset=dataset, version="CURRENT", partitions={}, ensure=True)
    write_dataset(df, Path(out_curr), basename="data")

    return {"rows": int(len(df)), "cols": list(df.columns)}


def _archive_inputs(
    *,
    attachment_path: Path,
    html_path: Path,
    run,
) -> Tuple[str, str]:
    """
    Copy raw files to uploads archive for provenance.
    """
    base = Path(
        paths.uploads_dir(
            category="invoices",
            partitions={"run_date": run.run_date, "run_id": run.run_id},
            ensure=True,
        )
    )
    att_out_dir = base / "attachments"
    html_out_dir = base / "email_html"
    att_out_dir.mkdir(parents=True, exist_ok=True)
    html_out_dir.mkdir(parents=True, exist_ok=True)

    out_att = att_out_dir / attachment_path.name
    out_html = html_out_dir / html_path.name

    shutil.copy2(attachment_path, out_att)
    shutil.copy2(html_path, out_html)

    return str(out_att), str(out_html)


def run_pipeline(*, run, stg_root: Path, source_system: str = "ARIBA_EMAIL_EXPORT") -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    stg_root = Path(stg_root)
    att_dir = stg_root / "attachments"
    html_dir = stg_root / "email_html"

    if not att_dir.exists():
        raise FileNotFoundError(f"Missing attachments dir: {att_dir}")
    if not html_dir.exists():
        raise FileNotFoundError(f"Missing email_html dir: {html_dir}")

    attachments = sorted([p for p in att_dir.glob("*.xlsx") if p.is_file()])
    html_files = sorted([p for p in html_dir.glob("*.html") if p.is_file()])

    metrics["inputs"] = {
        "attachments_count": len(attachments),
        "html_count": len(html_files),
        "stg_root": str(stg_root),
    }

    # Index HTML by message_id from filename: email__{message_id}_.html
    html_by_msg: Dict[str, Path] = {}
    for hp in html_files:
        m = re.match(r"^email__(?P<message_id>[^_]+)_\.html$", hp.name, re.IGNORECASE)
        if m:
            html_by_msg[m.group("message_id")] = hp

    file_manifest_rows: List[Dict[str, Any]] = []
    header_rows: List[Dict[str, Any]] = []
    line_frames: List[pd.DataFrame] = []

    orphan_attachments: List[str] = []
    orphan_html: List[str] = []
    parsed_ok = 0
    skipped_bad_floc = 0

    for ap in attachments:
        m = ATT_RE.match(ap.name)
        if not m:
            continue

        message_id = m.group("message_id")
        invoice_name = m.group("invoice_name")

        hp = html_by_msg.get(message_id)
        if hp is None or not hp.exists():
            orphan_attachments.append(ap.name)
            continue

        raw_html = _read_text(hp)

        # Parse HTML metadata (regex-only)
        meta = parse_ariba_email_html(raw_html)

        # Parse Excel
        header_kvs, raw_lines = read_invoice_excel_first_sheet(str(ap))
        lines = canonicalize_invoice_lines(raw_lines)

        # Optional hard validation: FLOC must start with OH-
        bad_floc = lines[~lines["floc_is_valid_oh"].fillna(False)]
        if len(bad_floc) > 0:
            file_manifest_rows.append(
                {
                    "message_id": message_id,
                    "attachment_file": ap.name,
                    "html_file": hp.name,
                    "invoice_name_from_attachment": invoice_name,
                    "run_date": run.run_date,
                    "run_id": run.run_id,
                    "source_system": source_system,
                    "status": "FAILED_VALIDATION_BAD_FLOC",
                    "bad_floc_sample": ";".join(bad_floc["floc_id"].astype("string").head(5).tolist()),
                }
            )
            skipped_bad_floc += 1
            continue

        # Archive raw inputs
        saved_att, saved_html = _archive_inputs(attachment_path=ap, html_path=hp, run=run)

        # Header row (no invoice_reconciliation_id)
        def hk(label: str) -> Optional[str]:
            return header_kvs.get(label.lower().strip())

        header_rows.append(
            {
                "message_id": message_id,
                "attachment_file": ap.name,
                "html_file": hp.name,
                "invoice_name_from_attachment": invoice_name,
                # from HTML (reliable)
                "supplier": meta.supplier,
                "supplier_invoice_number": meta.supplier_invoice_number,
                "on_behalf_of_preparer": meta.on_behalf_of_preparer,
                "invoice_date_html": meta.invoice_date,
                "company_code_html": meta.company_code,
                "total_amount_value_html": meta.total_amount_value,
                "total_amount_currency_html": meta.total_amount_currency,
                # from Excel (best-effort)
                "cwa_number": hk("cwa #"),
                "total_qty_structures": hk("total qty of structures"),
                "invoice_date_xlsx": hk("invoice date"),
                "invoice_number_xlsx": hk("invoice number"),
                "purchase_order_number": hk("purchase order no."),
                "change_order_number": hk("change order no."),
                "payment_terms": hk("payment terms"),
                "due_date": hk("due date"),
                "invoice_total_xlsx": hk("invoice total"),
                # lineage
                "run_date": run.run_date,
                "run_id": run.run_id,
                "source_system": source_system,
                "source_attachment_path": str(ap),
                "source_html_path": str(hp),
                "saved_attachment_path": saved_att,
                "saved_html_path": saved_html,
            }
        )

        # Line rows: add stable metadata columns
        lines2 = lines.copy()
        lines2.insert(0, "message_id", message_id)
        lines2.insert(1, "supplier", meta.supplier)
        lines2.insert(2, "supplier_invoice_number", meta.supplier_invoice_number)
        lines2.insert(3, "run_date", run.run_date)
        lines2.insert(4, "run_id", run.run_id)
        lines2.insert(5, "source_system", source_system)
        lines2.insert(6, "line_number", range(1, len(lines2) + 1))

        line_frames.append(lines2)

        # Manifest
        file_manifest_rows.append(
            {
                "message_id": message_id,
                "attachment_file": ap.name,
                "html_file": hp.name,
                "invoice_name_from_attachment": invoice_name,
                "source_attachment_path": str(ap),
                "source_html_path": str(hp),
                "saved_attachment_path": saved_att,
                "saved_html_path": saved_html,
                "run_date": run.run_date,
                "run_id": run.run_id,
                "source_system": source_system,
                "status": "PARSED_OK",
            }
        )

        parsed_ok += 1

    # HTML orphans (html with no attachment)
    att_msg_ids = set()
    for ap in attachments:
        m = ATT_RE.match(ap.name)
        if m:
            att_msg_ids.add(m.group("message_id"))

    for mid, p in html_by_msg.items():
        if mid not in att_msg_ids:
            orphan_html.append(p.name)

    metrics["orphans"] = {
        "orphan_attachments_count": len(orphan_attachments),
        "orphan_html_count": len(orphan_html),
        "orphan_attachments_sample": orphan_attachments[:10],
        "orphan_html_sample": orphan_html[:10],
    }
    metrics["parsed"] = {"parsed_ok": parsed_ok, "skipped_bad_floc": skipped_bad_floc}

    df_manifest = pd.DataFrame(file_manifest_rows)
    df_header = pd.DataFrame(header_rows)
    df_lines = pd.concat(line_frames, ignore_index=True, sort=False) if line_frames else pd.DataFrame()

    # Writes
    metrics["write_bronze_manifest"] = _write_whole_table(
        df_manifest,
        layer="bronze",
        dataset="vendor_invoices_file_manifest",
        run=run,
    )
    metrics["write_silver_header"] = _write_whole_table(
        df_header,
        layer="silver",
        dataset="vendor_invoice_header",
        run=run,
    )
    metrics["write_silver_line"] = _write_whole_table(
        df_lines,
        layer="silver",
        dataset="vendor_invoice_line",
        run=run,
    )

    return metrics
