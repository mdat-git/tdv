from __future__ import annotations

from pathlib import Path
import pandas as pd


def write_dataset(df: pd.DataFrame, out_dir: Path, *, basename: str = "data") -> Path:
    """
    Writes Parquet if pyarrow is installed; otherwise falls back to CSV.
    Returns the file path written.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Try parquet first
    parquet_path = out_dir / f"{basename}.parquet"
    try:
        df.to_parquet(parquet_path, index=False)  # requires pyarrow or fastparquet
        return parquet_path
    except Exception:
        # Fall back to CSV for now (still proves end-to-end pipeline)
        csv_path = out_dir / f"{basename}.csv"
        df.to_csv(csv_path, index=False)
        return csv_path
