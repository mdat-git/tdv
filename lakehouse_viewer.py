from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

# Optional (faster for huge Parquets)
try:
    import duckdb
except Exception:
    duckdb = None


# ----------------------------
# Config
# ----------------------------
DEFAULT_ROOT = Path("./LAKEHOUSE")  # change if needed
SUPPORTED_EXTS = {".parquet", ".pq", ".csv", ".json", ".xlsx", ".xls"}


# ----------------------------
# Helpers
# ----------------------------
def safe_resolve_under_root(root: Path, candidate: Path) -> Path:
    """Prevent path traversal outside root."""
    root = root.resolve()
    cand = candidate.resolve()
    if root not in cand.parents and cand != root:
        raise ValueError("Path is outside root.")
    return cand


def list_dir(root: Path, current: Path) -> Tuple[List[Path], List[Path]]:
    """Return (dirs, files) under current."""
    entries = list(current.iterdir())
    dirs = sorted([p for p in entries if p.is_dir()], key=lambda p: p.name.lower())
    files = sorted(
        [p for p in entries if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS],
        key=lambda p: p.name.lower(),
    )
    return dirs, files


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024.0:
            return f"{x:.1f} {u}"
        x /= 1024.0
    return f"{x:.1f} PB"


def parquet_metadata(path: Path) -> dict:
    if pq is None:
        return {"error": "pyarrow not installed; cannot read parquet metadata."}
    pf = pq.ParquetFile(path)
    md = pf.metadata
    schema = pf.schema_arrow
    return {
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "columns": schema.names,
        "schema": str(schema),
    }


@st.cache_data(show_spinner=False)
def load_preview(
    path: str,
    file_type: str,
    n_rows: int,
    use_duckdb: bool,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    p = Path(path)

    if file_type == "parquet":
        if use_duckdb and duckdb is not None:
            cols_sql = "*" if not columns else ", ".join([f'"{c}"' for c in columns])
            # read_parquet handles directories too if you pass a glob; here single file
            q = f"SELECT {cols_sql} FROM read_parquet('{p.as_posix()}') LIMIT {int(n_rows)}"
            return duckdb.query(q).to_df()

        # fallback: pyarrow/pandas
        df = pd.read_parquet(p)
        if columns:
            df = df[columns]
        return df.head(n_rows)

    if file_type == "csv":
        return pd.read_csv(p, nrows=n_rows)

    if file_type == "json":
        # tries line-delimited first, falls back to normal JSON
        try:
            return pd.read_json(p, lines=True).head(n_rows)
        except Exception:
            return pd.read_json(p).head(n_rows)

    if file_type == "excel":
        return pd.read_excel(p).head(n_rows)

    raise ValueError("Unsupported file type")


def infer_type(path: Path) -> str:
    s = path.suffix.lower()
    if s in {".parquet", ".pq"}:
        return "parquet"
    if s == ".csv":
        return "csv"
    if s == ".json":
        return "json"
    if s in {".xlsx", ".xls"}:
        return "excel"
    return "unknown"


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Lakehouse Viewer", layout="wide")
st.title("🗂️ Lakehouse File Explorer")

with st.sidebar:
    st.header("Root")
    root_in = st.text_input("Lakehouse root path", value=str(DEFAULT_ROOT))
    root = Path(root_in).expanduser()

    if not root.exists():
        st.error(f"Root does not exist: {root}")
        st.stop()

    st.divider()
    st.header("Preview Settings")
    n_rows = st.slider("Preview rows", 10, 2000, 200, step=10)
    use_duckdb = st.toggle("Use DuckDB for Parquet (faster for huge files)", value=True)

# Persist current directory + selected file
if "cwd" not in st.session_state:
    st.session_state.cwd = str(root)
if "selected_file" not in st.session_state:
    st.session_state.selected_file = None

cwd = safe_resolve_under_root(root, Path(st.session_state.cwd))

# Breadcrumbs
parts = cwd.relative_to(root).parts if cwd != root else ()
crumbs = [("LAKEHOUSE", root)]
acc = root
for part in parts:
    acc = acc / part
    crumbs.append((part, acc))

cols = st.columns(len(crumbs))
for i, (label, path) in enumerate(crumbs):
    if cols[i].button(label, use_container_width=True):
        st.session_state.cwd = str(path)
        st.session_state.selected_file = None
        st.rerun()

dirs, files = list_dir(root, cwd)

left, right = st.columns([0.42, 0.58])

with left:
    st.subheader("Folders")
    if cwd != root:
        if st.button("⬅️ Up one level", use_container_width=True):
            st.session_state.cwd = str(cwd.parent)
            st.session_state.selected_file = None
            st.rerun()

    if not dirs:
        st.caption("No subfolders here.")
    for d in dirs:
        if st.button(f"📁 {d.name}", key=f"dir:{d}", use_container_width=True):
            st.session_state.cwd = str(d)
            st.session_state.selected_file = None
            st.rerun()

    st.divider()
    st.subheader("Files")
    if not files:
        st.caption("No supported files here (.parquet/.csv/.json/.xlsx).")
    for f in files:
        size = human_bytes(f.stat().st_size)
        if st.button(f"📄 {f.name}  —  {size}", key=f"file:{f}", use_container_width=True):
            st.session_state.selected_file = str(f)
            st.rerun()

with right:
    st.subheader("Preview")
    if not st.session_state.selected_file:
        st.info("Select a file on the left to preview it.")
        st.stop()

    fpath = safe_resolve_under_root(root, Path(st.session_state.selected_file))
    ftype = infer_type(fpath)

    st.write(f"**File:** `{fpath}`")
    st.write(f"**Type:** `{ftype}`")

    # Metadata / Schema
    if ftype == "parquet":
        md = parquet_metadata(fpath)
        if "error" in md:
            st.warning(md["error"])
        else:
            with st.expander("Parquet metadata", expanded=True):
                st.write(f"**Rows:** {md['rows']:,}")
                st.write(f"**Row groups:** {md['row_groups']}")
                st.code(md["schema"], language="text")

            # Column picker for preview
            all_cols = md["columns"]
            selected_cols = st.multiselect(
                "Columns to preview (blank = all)",
                options=all_cols,
                default=[],
            )
    else:
        selected_cols = []

    # Load preview
    try:
        df = load_preview(
            path=str(fpath),
            file_type=ftype,
            n_rows=n_rows,
            use_duckdb=use_duckdb,
            columns=selected_cols if selected_cols else None,
        )
    except Exception as e:
        st.error(f"Failed to load preview: {e}")
        st.stop()

    st.dataframe(df, use_container_width=True, height=520)

    # Basic summary
    with st.expander("Quick summary"):
        st.write("**Shape (preview):**", df.shape)
        st.write("**Dtypes:**")
        st.dataframe(df.dtypes.astype(str).rename("dtype"), use_container_width=True)

    # Download preview
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download preview as CSV",
        data=csv_bytes,
        file_name=f"{fpath.stem}_preview.csv",
        mime="text/csv",
        use_container_width=True,
    )
