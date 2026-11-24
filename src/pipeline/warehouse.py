"""The warehouse: one DuckDB file holding pipeline metadata, gold tables and views over silver.

Schemas
  meta    run history, stage metrics, watermarks, file fingerprints, quality results
  silver  views over data/silver/<source>.parquet
  gold    materialised gold models (also exported to data/gold/<model>.parquet)
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .config import PipelineConfig
from .sqlutil import path_lit, q


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


@dataclass
class StageStats:
    """Metrics for one (source, stage) execution."""

    source: str
    stage: str  # extract | bronze | silver | gold
    status: str = "running"  # success | failed | skipped
    rows_in: int | None = None
    rows_out: int | None = None
    rows_quarantined: int | None = None
    rows_warn: int | None = None
    duplicates_dropped: int | None = None
    message: str = ""
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None


@dataclass
class QualityOutcome:
    source: str
    rule: str
    severity: str
    column: str | None
    violations: int
    outcome: str  # pass | warn | quarantined | fail


_DDL = [
    "CREATE SCHEMA IF NOT EXISTS meta",
    "CREATE SCHEMA IF NOT EXISTS silver",
    "CREATE SCHEMA IF NOT EXISTS gold",
    """CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
        run_id VARCHAR, trigger VARCHAR, args VARCHAR, status VARCHAR,
        started_at TIMESTAMP, finished_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS meta.stage_runs (
        run_id VARCHAR, source VARCHAR, stage VARCHAR, status VARCHAR,
        rows_in BIGINT, rows_out BIGINT, rows_quarantined BIGINT, rows_warn BIGINT,
        duplicates_dropped BIGINT, message VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS meta.watermarks (
        source VARCHAR, watermark VARCHAR, run_id VARCHAR, updated_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS meta.file_fingerprints (
        source VARCHAR, fingerprint VARCHAR, run_id VARCHAR, updated_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS meta.quality_results (
        run_id VARCHAR, source VARCHAR, rule VARCHAR, severity VARCHAR, column_name VARCHAR,
        violations BIGINT, outcome VARCHAR, checked_at TIMESTAMP)""",
]


class Warehouse:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        cfg.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.con = duckdb.connect(str(cfg.warehouse_path))
        except duckdb.IOException as exc:
            raise RuntimeError(
                f"cannot open {cfg.warehouse_path}: another process (a running pipeline, or a DuckDB "
                f"shell / DBeaver session) holds the file. Close it and retry. ({exc})"
            ) from exc
        for stmt in _DDL:
            self.con.execute(stmt)

    def close(self) -> None:
        self.con.close()

    def execute(self, sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyConnection:
        return self.con.execute(sql, params or [])

    # ------------------------------------------------------------------ runs

    def start_run(self, run_id: str, trigger: str, args: str) -> None:
        self.execute(
            "INSERT INTO meta.pipeline_runs VALUES (?, ?, ?, 'running', ?, NULL)",
            [run_id, trigger, args, utcnow()],
        )

    def finish_run(self, run_id: str, status: str) -> None:
        self.execute(
            "UPDATE meta.pipeline_runs SET status = ?, finished_at = ? WHERE run_id = ?",
            [status, utcnow(), run_id],
        )

    def record_stage(self, run_id: str, s: StageStats) -> None:
        self.execute(
            "INSERT INTO meta.stage_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [run_id, s.source, s.stage, s.status, s.rows_in, s.rows_out, s.rows_quarantined,
             s.rows_warn, s.duplicates_dropped, s.message[:2000], s.started_at, s.finished_at],
        )

    def record_quality(self, run_id: str, outcomes: list[QualityOutcome]) -> None:
        now = utcnow()
        for o in outcomes:
            self.execute(
                "INSERT INTO meta.quality_results VALUES (?,?,?,?,?,?,?,?)",
                [run_id, o.source, o.rule, o.severity, o.column, o.violations, o.outcome, now],
            )

    # -------------------------------------------------------- incremental state

    def _get(self, table: str, col: str, source: str) -> str | None:
        row = self.execute(f"SELECT {col} FROM meta.{table} WHERE source = ?", [source]).fetchone()
        return row[0] if row else None

    def _set(self, table: str, col: str, source: str, value: str, run_id: str) -> None:
        self.execute("BEGIN")
        try:
            self.execute(f"DELETE FROM meta.{table} WHERE source = ?", [source])
            self.execute(f"INSERT INTO meta.{table} VALUES (?, ?, ?, ?)", [source, value, run_id, utcnow()])
            self.execute("COMMIT")
        except Exception:
            self.execute("ROLLBACK")
            raise

    def get_watermark(self, source: str) -> str | None:
        return self._get("watermarks", "watermark", source)

    def set_watermark(self, source: str, value: str, run_id: str) -> None:
        self._set("watermarks", "watermark", source, value, run_id)

    def get_fingerprint(self, source: str) -> str | None:
        return self._get("file_fingerprints", "fingerprint", source)

    def set_fingerprint(self, source: str, value: str, run_id: str) -> None:
        self._set("file_fingerprints", "fingerprint", source, value, run_id)

    # ------------------------------------------------------------------ views

    def refresh_silver_view(self, source: str) -> None:
        path = self.cfg.silver_dir / f"{source}.parquet"
        if path.exists():
            self.execute(
                f"CREATE OR REPLACE VIEW silver.{q(source)} AS SELECT * FROM read_parquet({path_lit(path)})"
            )

    def refresh_all_views(self) -> None:
        for s in self.cfg.sources:
            self.refresh_silver_view(s.name)
        gold_dir: Path = self.cfg.gold_dir
        if gold_dir.exists():
            for f in sorted(gold_dir.glob("*.parquet")):
                if self.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='gold' AND table_name=?",
                    [f.stem],
                ).fetchone()[0] == 0:
                    self.execute(
                        f"CREATE OR REPLACE VIEW gold.{q(f.stem)} AS SELECT * FROM read_parquet({path_lit(f)})"
                    )
