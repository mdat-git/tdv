from __future__ import annotations

import hashlib
import pandas as pd


def row_hashes(df: pd.DataFrame) -> pd.Series:
    """
    Compute a stable row hash (good for delta diffs before you have a business key).
    - sorts columns for stability
    - converts values to string
    - treats NaN as empty string
    """
    df2 = df.copy()

    # stable column order
    df2 = df2.reindex(sorted(df2.columns), axis=1)

    # stable string representation
    s = df2.astype("string").fillna("").agg("|".join, axis=1)

    return s.map(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())
