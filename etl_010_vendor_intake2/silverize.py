# src/inspections_lakehouse/etl/etl_010_vendor_invoices_intake/silverize.py
from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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
    h = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", h)
    h = re.sub(r"(?is)</\s*(p|tr|td|div|table)\s*>", "\n", h)
    h = re.sub(r"(?is)<[^>]+>", "", h)
    h = html_lib.unescape(h)
    h = h.replace("\r", "\n")
    h = re.sub(r"\n+", "\n", h)

    lines: List[str] = []
    for line in h.split("\n"):
        t = _clean_str(line)
        if t:
            lines.append(t)
    return lines


def _extract_by_span_regex(raw_html: str, label: str) -> Optional[str]:
    """
    Try a tighter regex around typical Ariba pattern:
      <span ...>LABEL</span> ... <span ...>VALUE</span>
    """
    lab = re.escape(label)
    pat = re.compile(rf"(?is)>{lab}\s*</span>.*?>\s*([^<]+?)\s*</span>", re.IGNORECASE)
    m = pat.search(raw_html)
    if not m:
        return None
    val = _clean_str(m.group(1))
    return val or None


def _extract_by_lines(lines: List[str], label: str, *, known_labels: List[str]) -> Optional[str]:
    """
    Fallback: scan plaintext lines. When a line == LABEL, take the next line that
    isn't another label.
    """
    label_n = _norm_label(label)
    known = {_norm_label(x) for x in known_labels}

    for i, line in enumerate(lines):
        if _norm_label(line) == label_n:
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
    """
    if not s:
        return None
    t = _clean_str(s)

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

    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t)
    if m:
        return m.group(1)

    return t


# ----------------------------
# HTML Metadata (Regex-only)
# ----------------------------
KNOWN_LABELS = [
    "On behalf of / Preparer",
    "Supplier Invoice #",
    "Supplier",
    "Invoice Date",
    "Company Code",
    "Total Amount",
]


@dataclass(frozen=True)
class HtmlInvoiceMeta:
    on_behalf_of_preparer: Optional[str]
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
    supp_inv = get("Supplier Invoice #")
    supplier = get("Supplier")
    inv_date = _parse_date_loose(get("Invoice Date") or "")
    company_code = get("Company Code")

    total_raw = get("Total Amount") or ""
    total_val, total_cur = _parse_money(total_raw)

    return HtmlInvoiceMeta(
        on_behalf_of_preparer=on_behalf,
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
    Find 0-based header row that contains FLOC_ID / FLOC ID.
    """
    targets = {"floc_id", "floc id"}
    for r in range(len(preview)):
        row = preview.iloc[r].astype("string")
        vals = {_norm_label(v) for v in row.tolist() if v is not pd.NA}
        if any(v in targets for v in vals):
            return r
    return None


def read_invoice_excel_first_sheet(path_xlsx: str) -> Tuple[Dict[str, str], pd.DataFrame]:
    """
    Returns:
      header_kvs: dict of invoice header fields from top block (best-effort)
      lines_df: the line table starting at discovered header row
    """
    preview = pd.read_excel(path_xlsx, sheet_name=0, header=None, nrows=40, engine="openpyxl")
    hdr_row = _find_line_header_row(preview)
    if hdr_row is None:
        hdr_row = 12  # fallback to "line 13" convention (0-based index 12)

    header_block = pd.read_excel(path_xlsx, sheet_name=0, header=None, nrows=25, engine="openpyxl")
    header_kvs: Dict[str, str] = {}
    for i in range(len(header_block)):
        a = _clean_str(header_block.iat[i, 0]) if header_block.shape[1] > 0 else ""
        b = _clean_str(header_block.iat[i, 1]) if header_block.shape[1] > 1 else ""
        if a and b and len(a) <= 80:
            header_kvs[_norm_label(a)] = b

    lines_df = pd.read_excel(path_xlsx, sheet_name=0, header=hdr_row, engine="openpyxl")
    lines_df.columns = [str(c).strip() for c in lines_df.columns]
    return header_kvs, lines_df


# ----------------------------
# Canonicalize lines (drop raw caps columns)
# ----------------------------
def _extract_scope_id_from_block(block_id_series: pd.Series) -> pd.Series:
    """
    BLOCK_ID example: 'D2603-0001' -> scope_id 'D2603'
    """
    s = block_id_series.astype("string").fillna("").str.strip()
    out = s.str.extract(r"^([A-Z]\d{4})-", expand=False)
    out = out.astype("string")
    out = out.where(out != "", pd.NA)
    return out


def canonicalize_invoice_lines(lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output only canonical columns + a couple of validation flags.

    Canonical columns:
      floc_id, block_id, scope_id, scope_floc_key, folder, flight_date, upload_date,
      vendor_status, sce_struct, unit_rate

    Flags:
      floc_is_valid_oh, scope_floc_key_is_valid
    """
    df = lines_df.copy()

    # map normalized col -> actual
    norm_to_actual: Dict[str, str] = {}
    for c in df.columns:
        norm = re.sub(r"\s+", " ", str(c)).strip().lower().replace(" ", "_")
        norm_to_actual[norm] = c

    def pick(*cands: str) -> Optional[str]:
        for cand in cands:
            if cand in norm_to_actual:
                return norm_to_actual[cand]
        return None

    col_floc = pick("floc_id", "flocid", "floc")
    col_struct = pick("sce_struct", "sce_struct_", "sce_struct__")
    col_folder = pick("photo_loc", "photo_loc_", "photo_loc__")
    col_flight = pick("flight_date", "flightdate")
    col_upload = pick("upload_date", "uploaddate")
    col_vendor_status = pick("vendor_status", "vendorstatus")
    col_block = pick("block_id", "blockid")
    col_unit_rate = pick("unit_rate", "unitrate")

    out = pd.DataFrame(index=df.index)

    out["floc_id"] = df[col_floc].astype("string").str.strip() if col_floc else pd.NA
    out["sce_struct"] = df[col_struct].astype("string").str.strip() if col_struct else pd.NA
    out["folder"] = df[col_folder].astype("string").str.strip() if col_folder else pd.NA
    out["flight_date"] = df[col_flight].astype("string").str.strip() if col_flight else pd.NA
    out["upload_date"] = df[col_upload].astype("string").str.strip() if col_upload else pd.NA
    out["vendor_status"] = df[col_vendor_status].astype("string").str.strip() if col_vendor_status else pd.NA
    out["block_id"] = df[col_block].astype("string").str.strip() if col_block else pd.NA
    out["unit_rate"] = df[col_unit_rate].astype("string").str.strip() if col_unit_rate else pd.NA

    # Derived: scope_id
    out["scope_id"] = _extract_scope_id_from_block(out["block_id"].astype("string"))

    # Validate FLOC
    floc_clean = out["floc_id"].astype("string").fillna("").str.strip()
    out["floc_is_valid_oh"] = floc_clean.str.startswith("OH-")

    # scope_floc_key
    sid = out["scope_id"].astype("string").fillna("").str.strip()
    out["scope_floc_key"] = (sid + "|" + floc_clean).where((sid != "") & (floc_clean != ""), pd.NA)

    out["scope_floc_key_is_valid"] = out["scope_floc_key"].astype("string").fillna("").str.contains(r"^\w+\|OH-")

    # Drop blank flocs (trailing junk rows)
    out = out[floc_clean != ""].copy()

    return out.reset_index(drop=True)
