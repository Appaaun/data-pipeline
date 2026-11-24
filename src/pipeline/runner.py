"""Orchestration of one pipeline run.

Failure semantics
  * sources are isolated: one failing source never blocks the others
  * state (watermark / file fingerprint) is committed ONLY after silver succeeded, so a failed run is
    simply retried from the same point (at-least-once; silver de-duplicates on the primary key)
  * gold runs only if every selected source succeeded or was skipped as "nothing new"
"""

from __future__ import annotations

import logging
import shutil
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator

from .config import PipelineConfig, SourceConfig
from .errors import PipelineError
from .extractors import build_extractor
from .layers.bronze import list_bronze_runs, load_bronze
from .layers.gold import build_gold
from .layers.silver import build_silver
from .logging_setup import run_id_var
from .warehouse import StageStats, Warehouse, utcnow

log = logging.getLogger("pipeline.runner")


def new_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


@dataclass
class SourceOutcome:
    source: str
    status: str  # success | skipped | failed
    message: str = ""
    rows_in: int | None = None
    rows_out: int | None = None
    quarantined: int | None = None


@dataclass
class RunSummary:
    run_id: str
    status: str = "success"
    sources: list[SourceOutcome] = field(default_factory=list)
    gold: str = "not run"  # success | failed | skipped | not run
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == "success"


class Runner:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.wh = Warehouse(cfg)

    def close(self) -> None:
        self.wh.close()

    def __enter__(self) -> "Runner":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ helpers

    @contextmanager
    def _stage(self, run_id: str, source: str, stage: str) -> Iterator[StageStats]:
        st = StageStats(source=source, stage=stage, started_at=utcnow())
        try:
            yield st
            if st.status == "running":
                st.status = "success"
        except Exception as exc:
            st.status = "failed"
            st.message = f"{exc.__class__.__name__}: {exc}"
            raise
        finally:
            st.finished_at = utcnow()
            self.wh.record_stage(run_id, st)
            secs = (st.finished_at - st.started_at).total_seconds()
            log.debug("[%s] stage %s -> %s (%.1fs)", source, stage, st.status, secs)

    # ------------------------------------------------------------------- ingest

    def _ingest(self, run_id: str, src: SourceConfig, full_refresh: bool) -> SourceOutcome:
        wm = None if full_refresh else self.wh.get_watermark(src.name)
        fp = None if full_refresh else self.wh.get_fingerprint(src.name)
        extractor = build_extractor(src, self.cfg.root)

        with self._stage(run_id, src.name, "extract") as st:
            result = extractor.extract(self.cfg.landing_dir / src.name / run_id, watermark=wm, fingerprint=fp)
            st.rows_out = result.rows if result.path else 0
            if result.skipped:
                st.status, st.message = "skipped", result.reason
        if result.skipped:
            log.info("[%s] skipped: %s", src.name, result.reason)
            return SourceOutcome(src.name, "skipped", result.reason)
        assert result.path is not None

        with self._stage(run_id, src.name, "bronze") as st:
            bronze = load_bronze(self.wh, self.cfg, src, result.path, run_id)
            st.rows_in, st.rows_out = bronze.rows, bronze.rows

        replace = full_refresh or not src.is_incremental
        with self._stage(run_id, src.name, "silver") as st:
            res = build_silver(self.wh, self.cfg, src, run_id, [run_id], replace=replace)
            st.rows_in, st.rows_out = res.rows_in, res.rows_out
            st.rows_quarantined, st.rows_warn, st.duplicates_dropped = res.quarantined, res.warn_rows, res.duplicates_dropped

        # commit incremental state only now that the data is safely in silver
        if result.new_watermark is not None:
            self.wh.set_watermark(src.name, result.new_watermark, run_id)
        if result.fingerprint is not None:
            self.wh.set_fingerprint(src.name, result.fingerprint, run_id)
        return SourceOutcome(src.name, "success", "", res.rows_in, res.rows_out, res.quarantined)

    # ---------------------------------------------------------------------- run

    def run(
        self,
        sources: list[str] | None = None,
        *,
        skip_gold: bool = False,
        gold_only: bool = False,
        full_refresh: bool = False,
        trigger: str = "cli",
    ) -> RunSummary:
        selected = self.cfg.select(sources)
        run_id = new_run_id()
        token = run_id_var.set(run_id)
        t0 = time.monotonic()
        summary = RunSummary(run_id)
        args = f"sources={[s.name for s in selected]} skip_gold={skip_gold} gold_only={gold_only} full_refresh={full_refresh}"
        self.wh.start_run(run_id, trigger, args)
        log.info("run started: %s", args)
        try:
            if not gold_only:
                for src in selected:
                    try:
                        outcome = self._ingest(run_id, src, full_refresh)
                    except Exception as exc:  # isolate: keep going with the other sources
                        kind = "pipeline error" if isinstance(exc, PipelineError) else "unexpected error"
                        log.error("[%s] FAILED (%s): %s", src.name, kind, exc, exc_info=not isinstance(exc, PipelineError))
                        outcome = SourceOutcome(src.name, "failed", f"{exc.__class__.__name__}: {exc}")
                    summary.sources.append(outcome)

            sources_ok = all(o.status != "failed" for o in summary.sources)
            if skip_gold:
                summary.gold = "not run"
            elif not sources_ok:
                summary.gold = "skipped"
                log.warning("gold skipped because a source failed")
            else:
                try:
                    build_gold(self.wh, self.cfg, run_id)
                    summary.gold = "success"
                except PipelineError as exc:
                    log.error("gold failed: %s", exc)
                    summary.gold = "failed"

            self.wh.refresh_all_views()
            self._cleanup_landing()
            failed = any(o.status == "failed" for o in summary.sources) or summary.gold in {"failed"}
            summary.status = "failed" if failed else "success"
        except Exception:
            summary.status = "failed"
            log.exception("run aborted")
            raise
        finally:
            summary.seconds = time.monotonic() - t0
            self.wh.finish_run(run_id, summary.status)
            log.info("run finished: %s in %.1fs", summary.status, summary.seconds)
            run_id_var.reset(token)
        return summary

    # ---------------------------------------------------------------- reprocess

    def reprocess(self, source: str, run_ids: list[str] | None = None) -> RunSummary:
        """Rebuild silver for one source from bronze (after changing columns / quality rules).

        Full-load sources use the latest bronze snapshot; incremental sources replay ALL bronze runs
        (latest row per primary key wins). Pass explicit run ids to override.
        """
        src = self.cfg.source(source)
        available = list_bronze_runs(self.cfg, source)
        if not available:
            raise PipelineError(f"[{source}] no bronze data found - run the pipeline first")
        if run_ids:
            unknown = [r for r in run_ids if r not in available]
            if unknown:
                raise PipelineError(f"[{source}] unknown bronze run id(s): {unknown}")
            chosen = sorted(run_ids)
        else:
            chosen = available if src.is_incremental else available[-1:]

        run_id = new_run_id()
        token = run_id_var.set(run_id)
        summary = RunSummary(run_id)
        self.wh.start_run(run_id, "reprocess", f"source={source} bronze_runs={chosen}")
        try:
            with self._stage(run_id, source, "silver") as st:
                res = build_silver(self.wh, self.cfg, src, run_id, chosen, replace=True)
                st.rows_in, st.rows_out = res.rows_in, res.rows_out
                st.rows_quarantined, st.rows_warn, st.duplicates_dropped = res.quarantined, res.warn_rows, res.duplicates_dropped
            summary.sources.append(SourceOutcome(source, "success", "", res.rows_in, res.rows_out, res.quarantined))
        except Exception as exc:
            log.error("[%s] reprocess FAILED: %s", source, exc)
            summary.sources.append(SourceOutcome(source, "failed", f"{exc.__class__.__name__}: {exc}"))
            summary.status = "failed"
        finally:
            self.wh.finish_run(run_id, summary.status)
            run_id_var.reset(token)
        return summary

    # ------------------------------------------------------------------ cleanup

    def _cleanup_landing(self) -> None:
        """Landing is a transient buffer: bronze keeps the history, so old landing runs can go."""
        days = self.cfg.landing_retention_days
        if days < 0 or not self.cfg.landing_dir.exists():
            return
        cutoff = time.time() - days * 86400
        for run_dir in self.cfg.landing_dir.glob("*/*"):
            if run_dir.is_dir() and run_dir.stat().st_mtime < cutoff:
                shutil.rmtree(run_dir, ignore_errors=True)
                log.info("landing cleanup: removed %s", run_dir.relative_to(self.cfg.root))
