from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Literal

# ----------------------------
# Types
# ----------------------------
Backend = Literal["local", "snowflake"]
Layer = Literal["bronze", "silver", "gold", "util"]
Version = Literal["CURRENT", "HISTORY"]

# ----------------------------
# Env vars (keep these minimal)
# ----------------------------
ENV_BACKEND = "INSPECTIONS_BACKEND"                 # local | snowflake
ENV_LAKEHOUSE_ROOT = "INSPECTIONS_LAKEHOUSE_ROOT"   # local root for LAKEHOUSE
ENV_UPLOADS_ROOT = "INSPECTIONS_UPLOADS_ROOT"       # where raw incoming files are stored

# Snowflake (later)
ENV_SF_DATABASE = "INSPECTIONS_SF_DATABASE"         # e.g. TDINSPECTIONS_INFRA
ENV_SF_SCHEMA_BRONZE = "INSPECTIONS_SF_SCHEMA_BRONZE"
ENV_SF_SCHEMA_SILVER = "INSPECTIONS_SF_SCHEMA_SILVER"
ENV_SF_SCHEMA_GOLD = "INSPECTIONS_SF_SCHEMA_GOLD"
ENV_SF_SCHEMA_UTIL = "INSPECTIONS_SF_SCHEMA_UTIL"
ENV_SF_STAGE_BRONZE = "INSPECTIONS_SF_STAGE_BRONZE"  # e.g. @DB.BRONZE.BRONZE_STAGE

# ----------------------------
# Defaults (edit these once for your machine)
# ----------------------------
DEFAULT_LOCAL_LAKEHOUSE_ROOT = Path(r"C:\Users\YOUR_USER\OneDrive - YOUR_ORG\LAKEHOUSE")
DEFAULT_LOCAL_UPLOADS_ROOT = Path(r"C:\Users\YOUR_USER\OneDrive - YOUR_ORG\Inspections_Uploads")

DEFAULT_SF_DATABASE = "TDINSPECTIONS_INFRA"
DEFAULT_SF_SCHEMA_BRONZE = "BRONZE"
DEFAULT_SF_SCHEMA_SILVER = "SILVER"
DEFAULT_SF_SCHEMA_GOLD = "GOLD"
DEFAULT_SF_SCHEMA_UTIL = "UTIL"
DEFAULT_SF_STAGE_BRONZE = "@TDINSPECTIONS_INFRA.BRONZE.BRONZE_STAGE"


# ----------------------------
# Tiny helpers
# ----------------------------
def today_ymd() -> str:
    return date.today().isoformat()


def new_run_id() -> str:
    return uuid.uuid4().hex


def default_partitions(*, run_date: Optional[str] = None, run_id: Optional[str] = None) -> Dict[str, str]:
    """
    Standard partition keys for immutable outputs. ETLs should call this, not hardcode.
    """
    return {
        "run_date": run_date or today_ymd(),
        "run_id": run_id or new_run_id(),
    }


def _partitions_local(partitions: Optional[Dict[str, str]]) -> Path:
    """
    Convert {"k":"v"} into Path("k=v/k2=v2") with stable ordering.
    """
    if not partitions:
        return Path()
    parts = [f"{k}={partitions[k]}" for k in sorted(partitions.keys())]
    return Path(*parts)


def _partitions_stage(partitions: Optional[Dict[str, str]]) -> str:
    """
    Convert {"k":"v"} into "k=v/k2=v2" with stable ordering.
    """
    if not partitions:
        return ""
    return "/".join([f"{k}={partitions[k]}" for k in sorted(partitions.keys())])


# ----------------------------
# Paths object
# ----------------------------
@dataclass(frozen=True)
class Paths:
    backend: Backend
    lakehouse_root: Path
    uploads_root: Path

    # Snowflake config (later)
    sf_database: str
    sf_schema_bronze: str
    sf_schema_silver: str
    sf_schema_gold: str
    sf_schema_util: str
    sf_stage_bronze: str

    # ---- Local filesystem ----
    def local_dir(
        self,
        layer: Layer,
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
        ensure: bool = False,
    ) -> Path:
        """
        LAKEHOUSE/<layer>/(<subject>/)<dataset>/<CURRENT|HISTORY>/(k=v/...)?
        """
        p = self.lakehouse_root / layer
        if subject:
            p = p / subject
        p = p / dataset / version
        p = p / _partitions_local(partitions)
        if ensure:
            p.mkdir(parents=True, exist_ok=True)
        return p

    def uploads_dir(
        self,
        category: str,
        *,
        partitions: Optional[Dict[str, str]] = None,
        ensure: bool = False,
    ) -> Path:
        """
        UPLOADS/<category>/(k=v/...)?
        """
        p = self.uploads_root / category / _partitions_local(partitions)
        if ensure:
            p.mkdir(parents=True, exist_ok=True)
        return p

    # ---- Snowflake identifiers (later) ----
    def sf_table(self, layer: Literal["bronze", "silver", "gold", "util"], name: str) -> str:
        schema = {
            "bronze": self.sf_schema_bronze,
            "silver": self.sf_schema_silver,
            "gold": self.sf_schema_gold,
            "util": self.sf_schema_util,
        }[layer]
        return f"{self.sf_database}.{schema}.{name}"

    def sf_stage_prefix(
        self,
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        @STAGE/(<subject>/)<dataset>/<CURRENT|HISTORY>/(k=v/...)?
        """
        prefix = ""
        if subject:
            prefix += f"{subject}/"
        prefix += f"{dataset}/{version}"
        suffix = _partitions_stage(partitions)
        if suffix:
            prefix += f"/{suffix}"
        return f"{self.sf_stage_bronze}/{prefix}"

    # ---- Unified "where do I write dataset files?" ----
    def write_target(
        self,
        layer: Literal["bronze", "silver", "gold"],
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
        ensure_local: bool = False,
    ):
        """
        local -> Path
        snowflake -> stage prefix string
        """
        if self.backend == "local":
            return self.local_dir(
                layer, dataset, version, subject=subject, partitions=partitions, ensure=ensure_local
            )
        return self.sf_stage_prefix(dataset, version, subject=subject, partitions=partitions)


def get_paths() -> Paths:
    backend = os.getenv(ENV_BACKEND, "local").strip().lower()
    if backend not in ("local", "snowflake"):
        backend = "local"

    lakehouse_root = Path(os.getenv(ENV_LAKEHOUSE_ROOT) or DEFAULT_LOCAL_LAKEHOUSE_ROOT)
    uploads_root = Path(os.getenv(ENV_UPLOADS_ROOT) or DEFAULT_LOCAL_UPLOADS_ROOT)

    return Paths(
        backend=backend,
        lakehouse_root=lakehouse_root,
        uploads_root=uploads_root,
        sf_database=os.getenv(ENV_SF_DATABASE, DEFAULT_SF_DATABASE),
        sf_schema_bronze=os.getenv(ENV_SF_SCHEMA_BRONZE, DEFAULT_SF_SCHEMA_BRONZE),
        sf_schema_silver=os.getenv(ENV_SF_SCHEMA_SILVER, DEFAULT_SF_SCHEMA_SILVER),
        sf_schema_gold=os.getenv(ENV_SF_SCHEMA_GOLD, DEFAULT_SF_SCHEMA_GOLD),
        sf_schema_util=os.getenv(ENV_SF_SCHEMA_UTIL, DEFAULT_SF_SCHEMA_UTIL),
        sf_stage_bronze=os.getenv(ENV_SF_STAGE_BRONZE, DEFAULT_SF_STAGE_BRONZE),
    )


# Import this in every ETL:
paths = get_paths()
