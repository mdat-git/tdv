from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd


class ValidationError(Exception):
    pass


@dataclass(frozen=True)
class Contract:
    required_cols: List[str]
    not_null_cols: List[str]


def require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    required = list(required)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def require_not_null(df: pd.DataFrame, cols: Iterable[str]) -> None:
    cols = list(cols)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValidationError(f"Columns for not-null check are missing: {missing}")

    # Convert to a plain dict: {col: null_count}
    null_counts = df[cols].isna().sum().to_dict()

    bad = {c: int(null_counts.get(c, 0)) for c in cols if int(null_counts.get(c, 0)) > 0}
    if bad:
        raise ValidationError(f"Nulls found in not-null columns: {bad}")

def validate(df: pd.DataFrame, contract: Contract) -> None:
    require_columns(df, contract.required_cols)
    require_not_null(df, contract.not_null_cols)
