from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from inspections_lakehouse.util.paths import paths, default_partitions


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunLog:
    pipeline: str
    run_date: str
    run_id: str
    started_at_utc: str
    ended_at_utc: Optional[str] = None
    status: str = "RUNNING"  # RUNNING | SUCCESS | FAILED
    message: Optional[str] = None
    metrics: Dict[str, Any] = None

    @property
    def partitions(self) -> Dict[str, str]:
        return {"run_date": self.run_date, "run_id": self.run_id}


def start_run(pipeline: str, *, metrics: Optional[Dict[str, Any]] = None) -> RunLog:
    parts = default_partitions()
    run = RunLog(
        pipeline=pipeline,
        run_date=parts["run_date"],
        run_id=parts["run_id"],
        started_at_utc=_utc_now_iso(),
        metrics=metrics or {},
    )
    write_run(run)
    return run


def succeed_run(run: RunLog, *, message: Optional[str] = None, metrics: Optional[Dict[str, Any]] = None) -> None:
    run.status = "SUCCESS"
    run.ended_at_utc = _utc_now_iso()
    if message:
        run.message = message
    if metrics:
        run.metrics.update(metrics)
    write_run(run)


def fail_run(run: RunLog, *, message: str, metrics: Optional[Dict[str, Any]] = None) -> None:
    run.status = "FAILED"
    run.ended_at_utc = _utc_now_iso()
    run.message = message
    if metrics:
        run.metrics.update(metrics)
    write_run(run)


def run_log_path(run: RunLog) -> Path:
    # LAKEHOUSE/util/ingest_runs/CURRENT/run_date=.../run_id=.../run.json
    base = paths.local_dir(
        "util",
        "ingest_runs",
        "CURRENT",
        partitions=run.partitions,
        ensure=True,
    )
    return base / "run.json"


def write_run(run: RunLog) -> None:
    p = run_log_path(run)
    payload = asdict(run)
    payload["metrics"] = payload.get("metrics") or {}
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
