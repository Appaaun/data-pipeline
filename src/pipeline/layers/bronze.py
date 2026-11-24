"""Bronze: raw, append-only copy of what was extracted.

* every column is VARCHAR (no information is lost by typing too early)
* lineage columns added: _run_id, _ingested_at, _row_number, _row_hash
* one immutable parquet file per run: data/bronze/<source>/run_id=<id>/data.parquet
  => silver can always be rebuilt from bronze (`pipeline reprocess`)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from ..config import PipelineConfig, SourceConfig
from ..errors import ExtractError
from ..sqlutil import lit, path_lit
from ..warehouse import Warehouse, utcnow

log = logging.getLogger("pipeline.bronze")


@dataclass
class BronzeResult:
    path: Path
    rows: int
    columns: list[str]


def bronze_glob(cfg: PipelineConfig, source: str) -> Path:
    return cfg.bronze_dir / source / "run_id=*" / "data.parquet"


def list_bronze_runs(cfg: PipelineConfig, source: str) -> list[str]:
    """Run ids that have bronze data for this source, oldest first (run ids sort chronologically)."""
    base = cfg.bronze_dir / source
    if not base.exists():
        return []
    return sorted(p.name.removeprefix("run_id=") for p in base.glob("run_id=*") if (p / "data.parquet").exists())


def load_bronze(wh: Warehouse, cfg: PipelineConfig, source: SourceConfig, landing_file: Path, run_id: str) -> BronzeResult:
    dest = cfg.bronze_dir / source.name / f"run_id={run_id}" / "data.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.tmp")

    try:
        wh.execute(
            f"CREATE OR REPLACE TEMP VIEW _landing AS SELECT * FROM read_csv({path_lit(landing_file)}, "
            f"header = true, all_varchar = true, sample_size = -1)"
        )
    except Exception as exc:  # duckdb raises several subclasses for malformed files
        raise ExtractError(f"[{source.name}] cannot parse landing file {landing_file.name}: {exc}") from exc

    columns = [r[0] for r in wh.execute("DESCRIBE SELECT * FROM _landing").fetchall()]
    now = utcnow().isoformat(sep=" ")
    wh.execute(
        f"""COPY (
            SELECT t.*,
                   {lit(run_id)} AS _run_id,
                   CAST({lit(now)} AS TIMESTAMP) AS _ingested_at,
                   CAST(row_number() OVER () AS BIGINT) AS _row_number,
                   md5(CAST(t AS VARCHAR)) AS _row_hash
            FROM _landing t
        ) TO {path_lit(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    tmp.replace(dest)  # atomic publish: readers never see a half-written file
    rows = wh.execute(f"SELECT count(*) FROM read_parquet({path_lit(dest)})").fetchone()[0]
    wh.execute("DROP VIEW IF EXISTS _landing")
    log.info("[%s] bronze: %d rows -> %s", source.name, rows, dest.relative_to(cfg.root))
    return BronzeResult(path=dest, rows=rows, columns=columns)
