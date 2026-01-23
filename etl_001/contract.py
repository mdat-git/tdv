from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Keep permissive initially (empty lists).
# Tighten once you confirm the true key columns per sheet.
CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(required_cols=[], not_null_cols=[]),
    "Transmission": Contract(required_cols=[], not_null_cols=[]),
}
