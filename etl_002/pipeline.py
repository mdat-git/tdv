from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from inspections_lakehouse.util.paths import paths
from inspections_lakehouse.util.run_logging import RunLog
from inspections_lakehouse.util.validation import validate
from inspections_lakehouse.util.dataset_io import write_dataset
from inspections_lakehouse.util.vendors import validate_vendor

from inspections_lakehouse.etl.etl_002_scope_events_intake.contract import contracts_for
from inspections_lakehouse.etl.etl_002_scope_events_intake.silverize import (
    build_events_distribution_removal,
    build_events_transmission_removal,
    build_events_move_to_helo,
    write_events_line_and_header,
)

PIPELINE = "etl_002_scope_events_intake"

SHEETS = ["Distribution", "Transmission"]


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xlsx_path, sheet_name=sheet_name)


def run_pipeline(run: RunLog, *, input_path: str, vendor: str, event_type: str) -> None:
    vendor = validate_vendor(vendor)

    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    if event_type not in ("removal", "move_to_helo"):
        raise ValueError(f"event_type must be 'removal' or 'move_to_helo' (got: {event_type})")

    # Save exact artifact
    upload_parts = {"vendor": vendor, "event_type": event_type, **run.partitions}
    uploads_dir = paths.uploads_dir("scope_events_intake", partitions=upload_parts, ensure=True)
    saved_src = uploads_dir / src.name
    shutil.copy2(src, saved_src)

    run.metrics.update(
        {
            "vendor": vendor,
            "event_type": event_type,
            "source_file_original": str(src),
            "source_file_saved": str(saved_src),
        }
    )

    xl = pd.ExcelFile(saved_src)
    run.metrics["excel_sheet_names"] = xl.sheet_names

    contracts = contracts_for(event_type)

    all_events: list[pd.DataFrame] = []

    for sheet_name in SHEETS:
        if sheet_name not in xl.sheet_names:
            run.metrics[f"{sheet_name}__skipped"] = "missing sheet"
            continue

        df = _read_sheet(saved_src, sheet_name)
        validate(df, contracts[sheet_name])

        # Bronze raw (separate datasets per event type + sheet)
        dataset_raw = f"scope_events_{event_type}_{sheet_name.lower()}_raw"

        hist_dir = paths.local_dir(
            "bronze",
            dataset_raw,
            "HISTORY",
            partitions={"vendor": vendor, **run.partitions},
            ensure=True,
        )
        hist_file = write_dataset(df, hist_dir, basename="data")

        curr_dir = paths.local_dir(
            "bronze",
            dataset_raw,
            "CURRENT",
            partitions={"vendor": vendor},
            ensure=True,
        )
        curr_file = write_dataset(df, curr_dir, basename="data")

        run.metrics.update(
            {
                f"{dataset_raw}__rows": int(len(df)),
                f"{dataset_raw}__cols": int(df.shape[1]),
                f"{dataset_raw}__bronze_history_file": str(hist_file),
                f"{dataset_raw}__bronze_current_file": str(curr_file),
            }
        )

        # Build canonical event rows
        if event_type == "removal":
            if sheet_name == "Distribution":
                ev = build_events_distribution_removal(df, vendor=vendor, run=run, source_file_saved=str(saved_src))
            else:
                ev = build_events_transmission_removal(df, vendor=vendor, run=run, source_file_saved=str(saved_src))
        else:  # move_to_helo
            if sheet_name == "Distribution":
                ev = build_events_move_to_helo_distribution(
                    df, vendor=vendor, run=run, source_file_saved=str(saved_src)
                )
            else:  # Transmission
                ev = build_events_move_to_helo_transmission(
                    df, vendor=vendor, run=run, source_file_saved=str(saved_src)
                )


        all_events.append(ev)

    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    write_events_line_and_header(
        events_df=events_df,
        vendor=vendor,
        event_type=event_type,
        run=run,
        source_file_saved=str(saved_src),
    )
