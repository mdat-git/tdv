# src/inspections_lakehouse/etl/etl_010_vendor_invoices_intake/silverize.py
from __future__ import annotations

import re
import html as html_lib
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple, List

import pandas as pd


# ----------------------------
# Small helpers
# ----------------------------
def _clean_str(x: object) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\xa0", " ")
    return s.strip()


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", _clean_str(s)).strip().lower()


def _to_plain_lines(raw_html: str) -> List[str]:
    """
    Convert HTML -> list of normalized text lines.
    Regex-only: replace common separators with newlines, strip tags, unescape entities.
    """
    h = raw_html

    # separators -> newline
    h = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", h)
    h = re.sub(r"(?is)</\s*(p|tr|td|div|table)\s*>", "\n", h)

    # remove tags
    h = re.sub(r"(?is)<[^>]+>", "", h)

    # unescape & normalize
    h = html_lib.unescape(h)
    h = h.replace("\r", "\n")
    h = re.sub(r"\n+", "\n", h)

    lines = []
    for line in h.split("\n"):
        t = _clean_str(line)
        if t:
            lines.append(t)
    return lines


def _extract_by_span_regex(raw_html: str, label: str) -> Optional[str]:
    """
    Try a tighter regex around the common Ariba pattern:
      <span ...>LABEL</span> ... <span ...>VALUE</span>
    We search for the next "value-looking" span after the label span.
    """
    lab = re.escape(label)

    # Find LABEL inside a span, then capture the next span content as the value.
    # Keep it bounded so we don't jump across the whole email.
    pat = re.compile(
        rf"(?is)>{lab}\s*</span>.*?>\s*([^<]+?)\s*</span>",
        re.IGNORECASE,
    )
    m = pat.search(raw_html)
    if not m:
        return None
    val = _clean_str(m.group(1))
    return val or None


def _extract_by_lines(lines: List[str], label: str, *, known_labels: List[str]) -> Optional[str]:
    """
    Fallback: scan plaintext lines; whenever a line equals LABEL, pick the next line that
    isn't another label.
    If multiple occurrences exist, pick the first that yields a plausible value.
    """
    label_n = _norm_label(label)
    known = {_norm_label(x) for x in known_labels}

    for i, line in enumerate(lines):
        if _norm_label(line) == label_n:
            # next non-empty, not another label
            for j in range(i + 1, min(i + 6, len(lines))):
                nxt = lines[j]
                if _norm_label(nxt) in known:
                    break
                v = _clean_str(nxt)
                if v:
                    return v
    return None


def _parse_money(s: str) -> Tuple[Optional[float], Optional[str]]:
    """
    "$18,281.04 USD" -> (18281.04, "USD")
    """
    if not s:
        return None, None
    t = s.replace(",", "")
    cur = None
    mcur = re.search(r"\b([A-Z]{3})\b", t)
    if mcur:
        cur = mcur.group(1)
    m = re.search(r"(-?\d+(?:\.\d+)?)", t)
    if not m:
        return None, cur
    try:
        return float(m.group(1)), cur
    except Exception:
        return None, cur


def _parse_date_loose(s: str) -> Optional[str]:
    """
    Keep date as ISO string if parseable, else return original trimmed.
    Example: "Monday, January 26, 2026" -> "2026-01-26"
    """
    if not s:
        return None
    t = _clean_str(s)

    # common formats
    fmts = [
        "%A, %B %d, %Y",
        "%B %d, %Y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(t, fmt)
            return dt.date().isoformat()
        except Exception:
            pass

    # if it already contains yyyy-mm-dd, keep it
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", t):
        return re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t).group(1)  # type: ignore[union-attr]

    return t


# ----------------------------
# HTML Metadata (Regex-only)
# ----------------------------
KNOWN_LABELS = [
    "On behalf of / Preparer",
    "Invoice Reconciliation",
    "Supplier Invoice #",
    "Supplier",
    "Invoice Date",
    "Company Code",
    "Total Amount",
]


@dataclass(frozen=True)
class HtmlInvoiceMeta:
    on_behalf_of_preparer: Optional[str]
    invoice_reconciliation_id: Optional[str]
    supplier_invoice_number: Optional[str]
    supplier: Optional[str]
    invoice_date: Optional[str]
    company_code: Optional[str]
    total_amount_value: Optional[float]
    total_amount_currency: Optional[str]


def parse_ariba_email_html(raw_html: str) -> HtmlInvoiceMeta:
    """
    Extract reliable fields from Ariba email HTML using regex only.
    """
    lines = _to_plain_lines(raw_html)

    def get(label: str) -> Optional[str]:
        v = _extract_by_span_regex(raw_html, label)
        if v:
            return v
        return _extract_by_lines(lines, label, known_labels=KNOWN_LABELS)

    on_behalf = get("On behalf of / Preparer")
    ir_id = get("Invoice Reconciliation")
    supp_inv = get("Supplier Invoice #")
    supplier = get("Supplier")
    inv_date = _parse_date_loose(get("Invoice Date") or "")
    company_code = get("Company Code")

    total_raw = get("Total Amount") or ""
    total_val, total_cur = _parse_money(total_raw)

    return HtmlInvoiceMeta(
        on_behalf_of_preparer=on_behalf,
        invoice_reconciliation_id=ir_id,
        supplier_invoice_number=supp_inv,
        supplier=supplier,
        invoice_date=inv_date,
        company_code=company_code,
        total_amount_value=total_val,
        total_amount_currency=total_cur,
    )


# ----------------------------
# Excel parsing
# ----------------------------
def _find_line_header_row(preview: pd.DataFrame) -> Optional[int]:
    """
    Scan a header=None preview of the sheet and return the 0-based row index
    where the line header starts (contains FLOC_ID / FLOC ID).
    """
    target = {"floc_id", "floc id"}
    for r in range(len(preview)):
        row = preview.iloc[r].astype("string")
        vals = {_norm_label(v) for v in row.tolist() if v is not pd.NA}
        if any(v in target for v in vals):
            return r
    return None


def read_invoice_excel_first_sheet(path_xlsx: str) -> Tuple[Dict[str, str], pd.DataFrame]:
    """
    Returns:
      header_kvs: dict of invoice header fields from the top block (best-effort)
      lines_df: the line table starting at the discovered header row
    """
    # Preview first ~40 rows to find where line headers begin
    preview = pd.read_excel(path_xlsx, sheet_name=0, header=None, nrows=40, engine="openpyxl")
    hdr_row = _find_line_header_row(preview)

    # Header KV block: usually column A label, column B value in top area
    header_block = pd.read_excel(path_xlsx, sheet_name=0, header=None, nrows=25, engine="openpyxl")
    header_kvs: Dict[str, str] = {}
    for i in range(len(header_block)):
        a = _clean_str(header_block.iat[i, 0]) if header_block.shape[1] > 0 else ""
        b = _clean_str(header_block.iat[i, 1]) if header_block.shape[1] > 1 else ""
        if a and b and len(a) <= 60:
            header_kvs[_norm_label(a)] = b

    if hdr_row is None:
        # If we can't find it, fall back to the user hint (row 13 => index 12)
        hdr_row = 12

    lines_df = pd.read_excel(path_xlsx, sheet_name=0, header=hdr_row, engine="openpyxl")

    # normalize columns (strip, keep original but add a normalized map)
    lines_df.columns = [str(c).strip() for c in lines_df.columns]

    return header_kvs, lines_df


def canonicalize_invoice_lines(lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Make column names safe and consistent-ish, but do NOT assume order.
    We keep all columns, but we also create canonical aliases when possible.
    """
    df = lines_df.copy()

    # normalize col lookup
    norm_to_actual: Dict[str, str] = {}
    for c in df.columns:
        norm = _norm_label(str(c)).replace(" ", "_")
        norm_to_actual[norm] = c

    def pick(*cands: str) -> Optional[str]:
        for cand in cands:
            if cand in norm_to_actual:
                return norm_to_actual[cand]
        return None

    col_floc = pick("floc_id", "flocid", "floc")
    col_folder = pick("photo_loc", "photo_loc_", "photo_loc__")
    col_flight = pick("flight_date", "flightdate")
    col_upload = pick("upload_date", "uploaddate")
    col_vendor_status = pick("vendor_status", "vendorstatus")
    col_block = pick("block_id", "blockid")
    col_unit_rate = pick("unit_rate", "unitrate")

    if col_floc and col_floc != "floc_id":
        df["floc_id"] = df[col_floc].astype("string").str.strip()
    elif col_floc:
        df["floc_id"] = df[col_floc].astype("string").str.strip()
    else:
        df["floc_id"] = pd.NA

    if col_folder:
        df["folder"] = df[col_folder].astype("string").str.strip()
    if col_flight:
        df["flight_date"] = df[col_flight].astype("string").str.strip()
    if col_upload:
        df["upload_date"] = df[col_upload].astype("string").str.strip()
    if col_vendor_status:
        df["vendor_status"] = df[col_vendor_status].astype("string").str.strip()
    if col_block:
        df["block_id"] = df[col_block].astype("string").str.strip()
    if col_unit_rate:
        df["unit_rate"] = df[col_unit_rate].astype("string").str.strip()

    # Drop empty rows where floc is missing (typical trailing junk)
    df = df[df["floc_id"].astype("string").fillna("").str.strip() != ""].copy()

    return df.reset_index(drop=True)
