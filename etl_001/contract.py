from __future__ import annotations

from inspections_lakehouse.util.validation import Contract

# Start permissive; tighten once you confirm real column names.
# Add required columns as you standardize the intake schema.
CONTRACT = Contract(
    required_cols=[
        # "FLOC",  # example: uncomment once confirmed
    ],
    not_null_cols=[
        # "FLOC",  # example: uncomment once confirmed
    ],
)
