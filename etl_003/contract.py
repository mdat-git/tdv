from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(required_cols=["FLOC", "SCOPE_ID"], not_null_cols=["FLOC", "SCOPE_ID"]),
    "Transmission": Contract(required_cols=["FLOC", "SCOPE_ID"], not_null_cols=["FLOC", "SCOPE_ID"]),
}
