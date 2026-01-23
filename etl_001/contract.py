from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Keep permissive for now; tighten once you confirm true key columns per sheet.
CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(required_cols=[], not_null_cols=[]),
    "Transmission": Contract(required_cols=[], not_null_cols=[]),
}
