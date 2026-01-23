from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Keep permissive for now; tighten once you confirm true key columns per sheet.
CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(required_cols=[], not_null_cols=[]),
    "Transmission": Contract(required_cols=[], not_null_cols=[]),
}

# Canonical column names you want in Silver line
CANONICAL_COLS = [
    "scope_id",
    "floc",
    "scope_removal_date",
    "circuit",
    "city",
    "district",
    # add more as you need
]

# Map raw sheet columns -> canonical column names
RENAME_MAPS: dict[str, dict[str, str]] = {
    "Distribution": {
        "SCOPE_ID": "scope_id",
        "FLOC": "floc",
        "SCOPE_REMOVAL_DATE": "scope_removal_date",
        "Circuit": "circuit",
        "City": "city",
        "District": "district",
    },
    "Transmission": {
        "Scope ID": "scope_id",
        "FLOC_ID": "floc",
        "Scope Removal Date": "scope_removal_date",
        "Circuit": "circuit",
        "City": "city",
        "District": "district",
    },
}
