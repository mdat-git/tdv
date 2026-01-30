# src/inspections_lakehouse/etl/etl_010_vendor_invoice_intake/silverize.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup


# ----------------------------
# Common helpers
# ----------------------------

def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _norm_label(s: str) -> str:
    s2 = _norm_ws(s)
    s2 = s2.replace(" ", " ")
    return s2.strip().lower()


def _nul(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    s2 = _norm_ws(s)
    return s2 if s2 else None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v)
    s = s.replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _to_date_iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        dt = pd.to_datetime(str(v), errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date().isoformat()
    except Exception:
        return None


# ----------------------------
# HTML parsing (Ariba email body)
# ----------------------------

def _extract_label_value(soup: BeautifulSoup, label: str) -> Optional[str]:
    target = _norm_label(label)

    # Find exact-label spans, then take the next <tr> as value.
    for span in soup.find_all(["span", "p", "td"]):
        txt = _norm_label(span.get_text(" ", strip=True))
        if txt == target:
            tr = span.find_parent("tr")
            if tr is None:
                continue
            next_tr = tr.find_next_sibling("tr")
            if next_tr is None:
                continue
            val = _norm_ws(next_tr.get_text(" ", strip=True))
            return val if val else None

    return None


def parse_ariba_email_html(html_path: Path) -> Dict[str, Any]:
    """Parse Ariba email HTML and return key fields.

    Designed for the 'right-side' info box pattern:
      Label row -> Value row (next <tr>).

    Returns strings (raw) + a couple parsed numeric fields where safe.
    """
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    out: Dict[str, Any] = {}

    out["on_behalf_of_preparer"] = _extract_label_value(soup, "On behalf of / Preparer")
    out["invoice_reconciliation"] = _extract_label_value(soup, "Invoice Reconciliation")
    out["supplier_invoice_number"] = _extract_label_value(soup, "Supplier Invoice #")
    out["supplier"] = _extract_label_value(soup, "Supplier")
    out["invoice_date_text"] = _extract_label_value(soup, "Invoice Date")
    out["invoice_date"] = _to_date_iso(out["invoice_date_text"])
    out["company_code"] = _extract_label_value(soup, "Company Code")

    total_amount_text = _extract_label_value(soup, "Total Amount")
    out["total_amount_text"] = total_amount_text

    # Parse $18,281.04 USD -> amount, currency
    if total_amount_text:
        m = re.search(r"([A-Z]{3})\s*$", total_amount_text.strip())
        out["currency"] = m.group(1) if m else None
        out["total_amount"] = _to_float(total_amount_text)
    else:
        out["currency"] = None
        out["total_amount"] = None

    # Normalize blanks
    for k, v in list(out.items()):
        if isinstance(v, str):
            out[k] = _nul(v)

    return out


# ----------------------------
# Excel parsing (invoice file)
# ----------------------------

REQUIRED_LINE_COLS = [
    "FLOC_ID",
    "SCE_STRUCT",
    "PHOTO_LOC",
    "FLIGHT_DATE",
    "UPLOAD_DATE",
    "VENDOR_STATUS",
    "BLOCK_ID",
    "UNIT_RATE",
]


def _clean_cols(cols: list[str]) -> list[str]:
    out: list[str] = []
    for c in cols:
        c2 = _norm_ws(str(c))
        c2 = c2.replace("\n", " ")
        c2 = c2.replace("\r", " ")
        c2 = re.sub(r"\s+", " ", c2)
        out.append(c2)
    return out


def _find_header_row(path: Path, max_scan_rows: int = 80) -> int:
    """Find the header row index (0-based) that contains all REQUIRED_LINE_COLS."""
    df0 = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
    scan_n = min(max_scan_rows, len(df0))

    required = {c.strip().lower() for c in REQUIRED_LINE_COLS}

    for i in range(scan_n):
        row = df0.iloc[i].tolist()
        row_set = {str(x).strip().lower() for x in row if x is not None and str(x).strip() != ""}
        if required.issubset(row_set):
            return i

    raise ValueError(f"Could not locate line-table header row containing required columns: {REQUIRED_LINE_COLS}")


def _extract_header_kv(path: Path, max_scan_rows: int = 40, max_scan_cols: int = 8) -> Dict[str, Any]:
    """Extract invoice header key/value pairs from the top section.

    Looks for known labels anywhere in the top-left block and takes the cell to the right as value.
    """
    df0 = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
    df0 = df0.iloc[: max_scan_rows, : max_scan_cols]

    # canonical keys -> possible label variants
    labels = {
        "cwa_number": ["CWA #", "CWA#", "CWA"],
        "total_qty_structures": ["TOTAL QTY OF STRUCTURES", "TOTAL QTY", "TOTAL QTY OF STRUCTURE"],
        "invoice_date_excel": ["INVOICE DATE"],
        "invoice_number_excel": ["INVOICE NUMBER", "INVOICE #"],
        "purchase_order_number": ["PURCHASE ORDER NO.", "PURCHASE ORDER NO", "PO", "PO NUMBER"],
        "change_order_number": ["CHANGE ORDER NO.", "CHANGE ORDER NO", "CHANGE ORDER"],
        "payment_terms": ["PAYMENT TERMS"],
        "due_date_excel": ["DUE DATE"],
        "invoice_total_excel": ["INVOICE TOTAL", "TOTAL"],
    }

    # Build lookup map of normalized label -> canonical key
    want: Dict[str, str] = {}
    for key, variants in labels.items():
        for v in variants:
            want[_norm_label(v)] = key

    out: Dict[str, Any] = {}

    for r in range(df0.shape[0]):
        for c in range(df0.shape[1]):
            cell = df0.iat[r, c]
            if cell is None:
                continue
            cell_norm = _norm_label(str(cell))
            if cell_norm in want:
                key = want[cell_norm]

                # value: first non-empty cell to the right
                val = None
                for cc in range(c + 1, df0.shape[1]):
                    v = df0.iat[r, cc]
                    if v is not None and str(v).strip() != "":
                        val = v
                        break

                out[key] = _nul(val)

    # Parse dates + amounts where safe
    out["invoice_date_excel"] = _to_date_iso(out.get("invoice_date_excel"))
    out["due_date_excel"] = _to_date_iso(out.get("due_date_excel"))
    out["invoice_total_excel_amount"] = _to_float(out.get("invoice_total_excel"))

    # normalize structures count
    qty = out.get("total_qty_structures")
    if qty is not None:
        m = re.search(r"\d+", str(qty))
        out["total_qty_structures"] = m.group(0) if m else _nul(qty)

    return out


def parse_invoice_excel(path: Path) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Return (header_kv, line_df_raw) from the invoice excel."""
    header_row = _find_header_row(path)

    header_kv = _extract_header_kv(path)

    df_lines = pd.read_excel(path, sheet_name=0, header=header_row, dtype=str)

    # Clean column names
    df_lines.columns = _clean_cols(list(df_lines.columns))

    # Add raw row index for bronze dedupe/debug
    df_lines["_row_index"] = range(1, len(df_lines) + 1)

    # Drop rows that are completely empty across required cols
    def _row_is_empty(row) -> bool:
        for col in REQUIRED_LINE_COLS:
            if col in df_lines.columns:
                v = row.get(col)
                if v is not None and str(v).strip() != "":
                    return False
        return True

    df_lines = df_lines.loc[~df_lines.apply(_row_is_empty, axis=1)].copy()

    # Validation: required columns must exist
    missing = [c for c in REQUIRED_LINE_COLS if c not in df_lines.columns]
    if missing:
        raise ValueError(f"Missing required invoice line columns: {missing}")

    return header_kv, df_lines.reset_index(drop=True)


# ----------------------------
# Silverize
# ----------------------------

def silverize_invoice_header(
    *,
    excel_header_kv: Dict[str, Any],
    html_fields: Dict[str, Any],
    message_id: str,
    attachment_file: str,
    email_html_file: str,
    run,
    source_system: str,
    archive_paths: Dict[str, Any],
) -> pd.DataFrame:
    """Canonical header grain: 1 row per invoice attachment."""

    supplier = html_fields.get("supplier")

    invoice_number = excel_header_kv.get("invoice_number_excel") or html_fields.get("supplier_invoice_number")

    out = {
        "message_id": message_id,
        "supplier": _nul(supplier),
        "supplier_invoice_number": _nul(html_fields.get("supplier_invoice_number")),
        "invoice_reconciliation": _nul(html_fields.get("invoice_reconciliation")),
        "on_behalf_of_preparer": _nul(html_fields.get("on_behalf_of_preparer")),
        "company_code": _nul(html_fields.get("company_code")),
        "currency": _nul(html_fields.get("currency")),
        "ariba_total_amount": html_fields.get("total_amount"),
        "ariba_invoice_date": _nul(html_fields.get("invoice_date")),

        # Excel header
        "cwa_number": _nul(excel_header_kv.get("cwa_number")),
        "purchase_order_number": _nul(excel_header_kv.get("purchase_order_number")),
        "change_order_number": _nul(excel_header_kv.get("change_order_number")),
        "payment_terms": _nul(excel_header_kv.get("payment_terms")),
        "invoice_number_excel": _nul(excel_header_kv.get("invoice_number_excel")),
        "invoice_date_excel": _nul(excel_header_kv.get("invoice_date_excel")),
        "due_date_excel": _nul(excel_header_kv.get("due_date_excel")),
        "total_qty_structures": _nul(excel_header_kv.get("total_qty_structures")),
        "invoice_total_excel_amount": excel_header_kv.get("invoice_total_excel_amount"),

        # Canonical invoice number/date
        "invoice_number": _nul(invoice_number),
        "invoice_date": _nul(excel_header_kv.get("invoice_date_excel") or html_fields.get("invoice_date")),

        # Lineage
        "attachment_file": attachment_file,
        "email_html_file": email_html_file,
        "archived_attachment_path": _nul(archive_paths.get("attachment_saved_to")),
        "archived_email_html_path": _nul(archive_paths.get("email_html_saved_to")),
        "run_date": run.run_date,
        "run_id": run.run_id,
        "source_system": source_system,
    }

    return pd.DataFrame([out])


def silverize_invoice_lines(
    *,
    df_lines_raw: pd.DataFrame,
    header_row: Dict[str, Any],
    message_id: str,
    attachment_file: str,
    email_html_file: str,
    run,
    source_system: str,
) -> pd.DataFrame:
    """Canonical line grain: 1 row per FLOC line."""

    # Map exact headers (safe even if order changes)
    df = df_lines_raw.copy()

    # Standardize column names to match REQUIRED_LINE_COLS exactly (in case of whitespace)
    cols_norm = { _norm_label(c): c for c in df.columns }

    def _col(name: str) -> str:
        key = _norm_label(name)
        if key not in cols_norm:
            raise ValueError(f"Missing required column '{name}'")
        return cols_norm[key]

    out = pd.DataFrame()
    out["message_id"] = message_id
    out["invoice_number"] = header_row.get("invoice_number")
    out["supplier"] = header_row.get("supplier")

    out["floc_id"] = df[_col("FLOC_ID")].astype("string").str.strip()
    out["sce_struct"] = df[_col("SCE_STRUCT")].astype("string").str.strip()
    out["photo_loc"] = df[_col("PHOTO_LOC")].astype("string").str.strip()
    out["flight_date"] = df[_col("FLIGHT_DATE")].apply(_to_date_iso)
    out["upload_date"] = df[_col("UPLOAD_DATE")].apply(_to_date_iso)
    out["vendor_status"] = df[_col("VENDOR_STATUS")].astype("string").str.strip()
    out["block_id"] = df[_col("BLOCK_ID")].astype("string").str.strip()
    out["unit_rate"] = df[_col("UNIT_RATE")].apply(_to_float)

    # Line numbering (stable per attachment)
    out["line_number"] = range(1, len(out) + 1)

    # Lineage
    out["attachment_file"] = attachment_file
    out["email_html_file"] = email_html_file
    out["run_date"] = run.run_date
    out["run_id"] = run.run_id
    out["source_system"] = source_system

    # Drop blank flocs
    out["floc_id"] = out["floc_id"].where(out["floc_id"].fillna("") != "", pd.NA)
    out = out.dropna(subset=["floc_id"]).reset_index(drop=True)

    return out
