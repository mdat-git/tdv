from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Distribution: straightforward
# Transmission: requires at least one "Scope Package #N" column
CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(
        required_cols=["FLOC", "SCOPE_ID"],
        not_null_cols=["FLOC"],   # allow SCOPE_ID blanks -> COMP
    ),
    "Transmission": Contract(
        required_cols=["FLOC"],
        not_null_cols=["FLOC"],
        required_any_regex=[r"^Scope Package\s*#\s*\d+\s*$"],
    ),
}
