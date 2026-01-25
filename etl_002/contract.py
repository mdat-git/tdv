from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Transmission removals: must have at least one Scope Package #N column
TX_SCOPE_PKG_REGEX = r"^Scope Package\s*#\s*\d+\s*$"

REMOVAL_CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(
        required_cols=["FLOC", "SCOPE_ID"],
        not_null_cols=["FLOC"],  # allow blank SCOPE_ID -> COMP
        # removal date checked in silverize (more flexible name handling)
    ),
    "Transmission": Contract(
        required_cols=["FLOC"],
        not_null_cols=["FLOC"],
        required_any_regex=[TX_SCOPE_PKG_REGEX],
    ),
}

MOVE_TO_HELO_CONTRACTS: dict[str, Contract] = {
    "Distribution": Contract(required_cols=["FLOC", "SCOPE_ID"], not_null_cols=["FLOC", "SCOPE_ID"]),
    "Transmission": Contract(
        required_cols=["FLOC"],
        not_null_cols=["FLOC"],
        required_any_regex=[TX_SCOPE_PKG_REGEX],
    ),
}

def contracts_for(event_type: str) -> dict[str, Contract]:
    if event_type == "removal":
        return REMOVAL_CONTRACTS
    if event_type == "move_to_helo":
        return MOVE_TO_HELO_CONTRACTS
    raise ValueError(f"Unknown event_type: {event_type}")
