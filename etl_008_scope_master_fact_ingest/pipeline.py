# src/inspections_lakehouse/etl/etl_008_official_scope_master/pipeline.py
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.etl.etl_008_official_scope_master.silverize import (
    silverize_scope_table,
    build_floc_object_type_dim,
)

UPLOADS_CATEGORY = "official_scope"

# Bronze datasets (raw)
BRONZE_TRANS_HF = "official_scope_transmission_high_fire_raw"
BRONZE_DIST_HF = "official_scope_distribution_high_fire_raw"
BRONZE_DIST_NHF = "official_scope_distribution_non_high_fire_raw"

# Silver datasets (canonical)
SILVER_TRANS_HF = "official_scope_transmission_high_fire_line"
SILVER_DIST_HF = "official_scope_distribution_high_fire_line"
SILVER_DIST_NHF = "official_scope_distribution_non_high_fire_line"

# Consolidated dim (for ETL007 join)
SILVER_FLOC_DIM = "floc_object_type_dim"


def _read_xlsx(path: Path, *, sheet_name: Optional[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input XLSX not found: {path}")
    # dtype=str keeps things stable; you can parse later if needed
    return pd.read_excel(path, sheet_name=(sheet_name if sheet_name else 0), dtype=str)


def _archive_input(xlsx_path: Path, run, *, label: str) -> str:
    up_dir = paths.uploads_dir(
        category=UPLOADS_CATEGORY,
        partitions={"label": label, "run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    dest = Path(up_dir) / xlsx_path.name
    shutil.copy2(xlsx_path, dest)
    return str(dest)


def _write_whole_table(df: pd.DataFrame, *, layer: str, dataset: str, run) -> Dict[str, Any]:
    out_hist = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="HISTORY",
        partitions={"run_date": run.run_date, "run_id": run.run_id},
        ensure=True,
    )
    write_dataset(df, Path(out_hist), basename="data")

    out_curr = paths.local_dir(
        layer=layer,
        dataset=dataset,
        version="CURRENT",
        partitions={},
        ensure=True,
    )
    write_dataset(df, Path(out_curr), basename="data")

    return {"rows": int(len(df)), "cols": list(df.columns)}


def run_pipeline(
    *,
    run,
    trans_hf: Path,
    dist_hf: Path,
    dist_nhf: Path,
    sheet_name: Optional[str],
    source_system: str = "OFFICIAL_SCOPE_SPREADSHEET",
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # ----------------------------
    # Read + archive inputs
    # ----------------------------
    df_trans_raw = _read_xlsx(trans_hf, sheet_name=sheet_name)
    df_dist_hf_raw = _read_xlsx(dist_hf, sheet_name=sheet_name)
    df_dist_nhf_raw = _read_xlsx(dist_nhf, sheet_name=sheet_name)

    metrics["read"] = {
        "trans_hf_rows": int(len(df_trans_raw)),
        "dist_hf_rows": int(len(df_dist_hf_raw)),
        "dist_nhf_rows": int(len(df_dist_nhf_raw)),
    }

    trans_saved = _archive_input(trans_hf, run, label="trans_hf")
    dist_hf_saved = _archive_input(dist_hf, run, label="dist_hf")
    dist_nhf_saved = _archive_input(dist_nhf, run, label="dist_nhf")

    metrics["archived"] = {
        "trans_hf_saved": trans_saved,
        "dist_hf_saved": dist_hf_saved,
        "dist_nhf_saved": dist_nhf_saved,
    }

    # ----------------------------
    # Bronze (raw + minimal lineage)
    # ----------------------------
    def bronzeize(df: pd.DataFrame, saved: str, asset_class: str, scope_list: str) -> pd.DataFrame:
        out = df.copy()
        out["asset_class"] = asset_class
        out["scope_list"] = scope_list
        out["source_system"] = source_system
        out["source_file_saved"] = saved
        out["run_date"] = run.run_date
        out["run_id"] = run.run_id
        return out

    b_trans = bronzeize(df_trans_raw, trans_saved, asset_class="transmission", scope_list="HIGH_FIRE")
    b_dist_hf = bronzeize(df_dist_hf_raw, dist_hf_saved, asset_class="distribution", scope_list="HIGH_FIRE")
    b_dist_nhf = bronzeize(df_dist_nhf_raw, dist_nhf_saved, asset_class="distribution", scope_list="NON_HIGH_FIRE")

    metrics["write_bronze_trans_hf"] = _write_whole_table(b_trans, layer="bronze", dataset=BRONZE_TRANS_HF, run=run)
    metrics["write_bronze_dist_hf"] = _write_whole_table(b_dist_hf, layer="bronze", dataset=BRONZE_DIST_HF, run=run)
    metrics["write_bronze_dist_nhf"] = _write_whole_table(b_dist_nhf, layer="bronze", dataset=BRONZE_DIST_NHF, run=run)

    # ----------------------------
    # Silver (canonical selection)
    # ----------------------------
    s_trans = silverize_scope_table(
        b_trans,
        run=run,
        asset_class="transmission",
        scope_list="HIGH_FIRE",
        source_system=source_system,
        source_file_saved=trans_saved,
    )
    s_dist_hf = silverize_scope_table(
        b_dist_hf,
        run=run,
        asset_class="distribution",
        scope_list="HIGH_FIRE",
        source_system=source_system,
        source_file_saved=dist_hf_saved,
    )
    s_dist_nhf = silverize_scope_table(
        b_dist_nhf,
        run=run,
        asset_class="distribution",
        scope_list="NON_HIGH_FIRE",
        source_system=source_system,
        source_file_saved=dist_nhf_saved,
    )

    metrics["write_silver_trans_hf"] = _write_whole_table(s_trans, layer="silver", dataset=SILVER_TRANS_HF, run=run)
    metrics["write_silver_dist_hf"] = _write_whole_table(s_dist_hf, layer="silver", dataset=SILVER_DIST_HF, run=run)
    metrics["write_silver_dist_nhf"] = _write_whole_table(s_dist_nhf, layer="silver", dataset=SILVER_DIST_NHF, run=run)

    # ----------------------------
    # Consolidated dim: floc_object_type_dim
    #   - union all 3 silver tables
    #   - choose "best" object_type deterministically
    # ----------------------------
    dim = build_floc_object_type_dim(pd.concat([s_trans, s_dist_hf, s_dist_nhf], ignore_index=True))
    dim["run_date"] = run.run_date
    dim["run_id"] = run.run_id
    dim["source_system"] = "OFFICIAL_SCOPE_MASTER"

    metrics["write_silver_floc_dim"] = _write_whole_table(dim, layer="silver", dataset=SILVER_FLOC_DIM, run=run)

    return metrics
