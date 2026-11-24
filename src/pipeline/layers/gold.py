"""Gold: business-facing tables built from SQL files in sql/gold/.

Each file is one model: a single SELECT. Header comments declare metadata:

    -- description: Stock value and risk per category
    -- depends: sales_velocity, other_model      (only needed for gold -> gold dependencies)

Models read silver via `silver.<source>`, other gold models via `gold.<model>`, and run history
via `meta.*`. They are materialised as tables in the warehouse and exported to data/gold/<model>.parquet.
Models run in dependency order; a failed model skips its dependents but not unrelated models.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

from ..config import PipelineConfig
from ..errors import ConfigError, GoldError
from ..sqlutil import path_lit, q
from ..warehouse import StageStats, Warehouse, utcnow

log = logging.getLogger("pipeline.gold")

_DEPENDS_RE = re.compile(r"^\s*--\s*depends\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_DESC_RE = re.compile(r"^\s*--\s*description\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)


@dataclass(frozen=True)
class GoldModel:
    name: str
    sql: str
    depends: tuple[str, ...]
    description: str


def discover_models(sql_dir: Path) -> list[GoldModel]:
    """Parse sql/gold/*.sql and return the models in dependency order."""
    if not sql_dir.exists():
        return []
    models: dict[str, GoldModel] = {}
    for f in sorted(sql_dir.glob("*.sql")):
        text = f.read_text(encoding="utf-8")
        deps = tuple(
            d.strip() for m in _DEPENDS_RE.finditer(text) for d in m.group(1).split(",") if d.strip()
        )
        desc = _DESC_RE.search(text)
        body = text.strip().rstrip(";").strip()
        models[f.stem] = GoldModel(f.stem, body, deps, desc.group(1).strip() if desc else "")
    for m in models.values():
        for d in m.depends:
            if d not in models:
                raise ConfigError(f"gold model '{m.name}' depends on unknown model '{d}'")
    try:
        order = list(TopologicalSorter({m.name: set(m.depends) for m in models.values()}).static_order())
    except CycleError as exc:
        raise ConfigError(f"gold models have a dependency cycle: {exc.args[1]}") from exc
    return [models[n] for n in order]


def build_gold(wh: Warehouse, cfg: PipelineConfig, run_id: str) -> list[StageStats]:
    models = discover_models(cfg.gold_sql_dir)
    if not models:
        log.warning("no gold models found in %s", cfg.gold_sql_dir)
    cfg.gold_dir.mkdir(parents=True, exist_ok=True)
    failed: set[str] = set()
    stats: list[StageStats] = []

    for m in models:
        st = StageStats(source=m.name, stage="gold", started_at=utcnow())
        blocked = [d for d in m.depends if d in failed]
        if blocked:
            st.status, st.message = "skipped", f"dependency failed: {', '.join(blocked)}"
            failed.add(m.name)
            log.warning("[%s] skipped (%s)", m.name, st.message)
        else:
            try:
                out = cfg.gold_dir / f"{m.name}.parquet"
                tmp = out.with_suffix(".parquet.tmp")
                kind = wh.execute(
                    "SELECT table_type FROM information_schema.tables WHERE table_schema = 'gold' AND table_name = ?",
                    [m.name],
                ).fetchone()
                if kind and kind[0] == "VIEW":  # left over from refresh_all_views() on a fresh warehouse
                    wh.execute(f"DROP VIEW gold.{q(m.name)}")
                wh.execute(f"CREATE OR REPLACE TABLE gold.{q(m.name)} AS {m.sql}")
                st.rows_out = wh.execute(f"SELECT count(*) FROM gold.{q(m.name)}").fetchone()[0]
                wh.execute(f"COPY gold.{q(m.name)} TO {path_lit(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
                tmp.replace(out)
                st.status = "success"
                if st.rows_out == 0:
                    st.message = "model produced 0 rows"
                    log.warning("[%s] gold model produced 0 rows", m.name)
                log.info("[%s] gold: %d rows", m.name, st.rows_out)
            except Exception as exc:
                st.status, st.message = "failed", f"{exc.__class__.__name__}: {exc}"
                failed.add(m.name)
                log.error("[%s] gold model failed: %s", m.name, exc)
        st.finished_at = utcnow()
        wh.record_stage(run_id, st)
        stats.append(st)
    if any(s.status == "failed" for s in stats):
        names = [s.source for s in stats if s.status == "failed"]
        raise GoldError(f"gold model(s) failed: {', '.join(names)}")
    return stats
