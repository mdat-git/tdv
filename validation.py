# inspections_lakehouse/util/validation.py
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Pattern

import pandas as pd


@dataclass(frozen=True)
class Contract:
    required_cols: List[str] = field(default_factory=list)
    not_null_cols: List[str] = field(default_factory=list)

    # NEW: require at least one column matching each regex pattern
    required_any_regex: List[str] = field(default_factory=list)


def _col_lookup(df: pd.DataFrame) -> dict[str, str]:
    # case-insensitive map: lower -> original
    return {str(c).strip().lower(): str(c).strip() for c in df.columns}


def validate(df: pd.DataFrame, contract: Contract) -> None:
    cols_map = _col_lookup(df)
    actual_cols = set(cols_map.keys())

    # required_cols (case-insensitive)
    missing_required = []
    for c in contract.required_cols:
        if c.strip().lower() not in actual_cols:
            missing_required.append(c)

    if missing_required:
        raise KeyError(f"Missing required columns: {missing_required}. Available: {list(df.columns)}")

    # required_any_regex: for each pattern, at least one matching col must exist
    for pat in contract.required_any_regex:
        rx = re.compile(pat, flags=re.IGNORECASE)
        matches = [orig for orig in df.columns if rx.match(str(orig).strip())]
        if not matches:
            raise KeyError(
                f"Missing required patterned column. Need at least one matching regex '{pat}'. "
                f"Available: {list(df.columns)}"
            )

    # not_null_cols (case-insensitive)
    missing_notnull = []
    for c in contract.not_null_cols:
        key = c.strip().lower()
        if key not in cols_map:
            missing_notnull.append(c)
            continue

        col = cols_map[key]
        if df[col].isna().all():
            raise ValueError(f"Column '{col}' is entirely null, expected values.")

    if missing_notnull:
        raise KeyError(f"Columns listed in not_null_cols missing from df: {missing_notnull}")
