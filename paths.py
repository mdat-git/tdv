from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Literal

Backend = Literal["local", "snowflake"]
Version = Literal["CURRENT", "HISTORY"]

# ---- env knobs (keep these few + obvious) ----
ENV_BACKEND = "INSPECTIONS_BACKEND"                 # local | snowflake
ENV_LAKEHOUSE_ROOT = "INSPECTIONS_LAKEHOUSE_ROOT"   # default: <repo>/LAKEHOUSE

ENV_SF_DATABASE = "INSPECTIONS_SF_DATABASE"         # e.g. TDINSPECTIONS_INFRA
ENV_SF_SCHEMA_BRONZE = "INSPECTIONS_SF_SCHEMA_BRONZE"  # default BRONZE
ENV_SF_SCHEMA_SILVER = "INSPECTIONS_SF_SCHEMA_SILVER"  # default SILVER
ENV_SF_SCHEMA_GOLD = "INSPECTIONS_SF_SCHEMA_GOLD"      # default GOLD
ENV_SF_SCHEMA_UTIL = "INSPECTIONS_SF_SCHEMA_UTIL"      # default UTIL

ENV_SF_STAGE_BRONZE = "INSPECTIONS_SF_STAGE_BRONZE"    # e.g. @TD...BRONZE.BRONZE_STAGE
ENV_SF_STAGE_SILVER = "INSPECTIONS_SF_STAGE_SILVER"    # optional
ENV_SF_STAGE_GOLD = "INSPECTIONS_SF_STAGE_GOLD"        # optional


def _slug(s: str) -> str:
    """Filesystem + identifier safe-ish: letters/numbers/_ only (simple on purpose)."""
    s = s.strip().replace(" ", "_")
    s = re.sub(r"[^A-Za-z0-9_]+", "", s)
    return s


def _repo_root() -> Path:
    """
    Find repo root by walking up until pyproject.toml exists.
    Simple + reliable, avoids hardcoding.
    """
    cur = Path(__file__).resolve()
    for _ in range(20):
        if (cur / "pyproject.toml").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # If this fails, user can set INSPECTIONS_LAKEHOUSE_ROOT explicitly.
    return Path.cwd().resolve()


def _layer_to_schema(layer: str, bronze: str, silver: str, gold: str, util: str) -> str:
    layer = layer.lower()
    if layer == "bronze":
        return bronze
    if layer == "silver":
        return silver
    if layer == "gold":
        return gold
    if layer == "util":
        return util
    raise ValueError(f"Invalid layer: {layer}. Use bronze/silver/gold/util.")


def _layer_to_stage(layer: str, bronze: str, silver: str, gold: str) -> str:
    layer = layer.lower()
    if layer == "bronze":
        return bronze
    if layer == "silver":
        return silver
    if layer == "gold":
        return gold
    raise ValueError(f"Invalid stage layer: {layer}. Use bronze/silver/gold.")


def _partition_suffix(partitions: Optional[Dict[str, str]]) -> str:
    """
    Returns: /k=v/k2=v2 (stable order for traceability)
    """
    if not partitions:
        return ""
    parts = []
    for k in sorted(partitions.keys()):
        parts.append(f"{_slug(k)}={_slug(str(partitions[k]))}")
    return "/" + "/".join(parts)


@dataclass(frozen=True)
class Paths:
    """
    One object. Simple methods.
    - local_dir(...) -> Path
    - sf_table(...) -> "DB.SCHEMA.TABLE"
    - sf_stage_uri(...) -> "@DB.SCHEMA.STAGE/prefix/..."
    """

    backend: Backend
    lakehouse_root: Path

    # Snowflake naming
    sf_database: str
    sf_schema_bronze: str
    sf_schema_silver: str
    sf_schema_gold: str
    sf_schema_util: str

    sf_stage_bronze: str
    sf_stage_silver: str
    sf_stage_gold: str

    # -------- local filesystem --------
    def local_dir(
        self,
        layer: str,
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
        ensure: bool = False,
    ) -> Path:
        """
        <LAKEHOUSE>/<layer>/(<subject>/)<dataset>/<CURRENT|HISTORY>/(k=v/...)?
        """
        layer = layer.lower()
        dataset = _slug(dataset)
        base = self.lakehouse_root / layer
        if subject:
            base = base / _slug(subject)
        p = base / dataset / version
        if partitions:
            for k in sorted(partitions.keys()):
                p = p / f"{_slug(k)}={_slug(str(partitions[k]))}"
        if ensure:
            p.mkdir(parents=True, exist_ok=True)
        return p

    # -------- snowflake identifiers --------
    def sf_table(
        self,
        layer: str,
        dataset: str,
        *,
        subject: Optional[str] = None,
        version: Optional[Version] = None,
    ) -> str:
        """
        Returns fully qualified table name.
        Convention (simple + consistent):
          DB.<SCHEMA>.<SUBJECT__DATASET>  (subject optional)
        You can optionally include version in the name if you want (rare for tables):
          ..._<CURRENT|HISTORY>
        """
        schema = _layer_to_schema(
            layer,
            bronze=self.sf_schema_bronze,
            silver=self.sf_schema_silver,
            gold=self.sf_schema_gold,
            util=self.sf_schema_util,
        )
        name = _slug(dataset)
        if subject:
            name = f"{_slug(subject)}__{name}"
        if version:
            name = f"{name}__{version}"
        return f"{self.sf_database}.{schema}.{name}"

    def sf_stage_uri(
        self,
        layer: str,
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
        filename: Optional[str] = None,
    ) -> str:
        """
        Returns a stage URI like:
          @DB.SCHEMA.STAGE/subject/dataset/CURRENT/k=v/.../file.parquet
        """
        stage = _layer_to_stage(
            layer,
            bronze=self.sf_stage_bronze,
            silver=self.sf_stage_silver,
            gold=self.sf_stage_gold,
        )
        dataset = _slug(dataset)

        prefix = ""
        if subject:
            prefix += f"{_slug(subject)}/"
        prefix += f"{dataset}/{version}"
        prefix += _partition_suffix(partitions)

        if filename:
            filename = filename.strip().lstrip("/")

        return f"{stage}/{prefix}" + (f"/{filename}" if filename else "")

    # -------- unified helpers (what ETLs should call) --------
    def dataset_location(
        self,
        layer: str,
        dataset: str,
        version: Version = "CURRENT",
        *,
        subject: Optional[str] = None,
        partitions: Optional[Dict[str, str]] = None,
        ensure_local: bool = False,
    ):
        """
        One call for ETLs when you just want "where do I write?"
        - local backend -> Path
        - snowflake backend -> stage URI string
        """
        if self.backend == "local":
            return self.local_dir(
                layer, dataset, version, subject=subject, partitions=partitions, ensure=ensure_local
            )
        # snowflake default for file-like writes: stage URI
        return self.sf_stage_uri(layer, dataset, version, subject=subject, partitions=partitions)


def get_paths() -> Paths:
    backend = (os.getenv(ENV_BACKEND, "local") or "local").strip().lower()
    if backend not in ("local", "snowflake"):
        raise ValueError(f"{ENV_BACKEND} must be 'local' or 'snowflake' (got '{backend}')")

    # local root
    override = os.getenv(ENV_LAKEHOUSE_ROOT)
    if override:
        root = Path(override).expanduser()
        root = root if root.is_absolute() else (_repo_root() / root).resolve()
    else:
        root = (_repo_root() / "LAKEHOUSE").resolve()

    # snowflake defaults
    sf_db = os.getenv(ENV_SF_DATABASE, "TDINSPECTIONS_INFRA")
    sf_b = os.getenv(ENV_SF_SCHEMA_BRONZE, "BRONZE")
    sf_s = os.getenv(ENV_SF_SCHEMA_SILVER, "SILVER")
    sf_g = os.getenv(ENV_SF_SCHEMA_GOLD, "GOLD")
    sf_u = os.getenv(ENV_SF_SCHEMA_UTIL, "UTIL")

    # stages: you can leave silver/gold as bronze stage if you want (simple default)
    stage_b = os.getenv(ENV_SF_STAGE_BRONZE, "@TDINSPECTIONS_INFRA.BRONZE.BRONZE_STAGE")
    stage_s = os.getenv(ENV_SF_STAGE_SILVER, stage_b)
    stage_g = os.getenv(ENV_SF_STAGE_GOLD, stage_b)

    return Paths(
        backend=backend, lakehouse_root=root,
        sf_database=sf_db,
        sf_schema_bronze=sf_b, sf_schema_silver=sf_s, sf_schema_gold=sf_g, sf_schema_util=sf_u,
        sf_stage_bronze=stage_b, sf_stage_silver=stage_s, sf_stage_gold=stage_g,
    )


# The one import everyone uses:
paths = get_paths()
