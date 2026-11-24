"""Command line interface:  pipeline <command>  (or: python -m pipeline <command>)"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence

from .config import PipelineConfig, load_config
from .errors import ConfigError, PipelineError
from .logging_setup import setup_logging

DEFAULT_CONFIG = "config/pipeline.yaml"


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]], max_width: int = 60) -> str:
    def cell(v: Any) -> str:
        s = "" if v is None else str(v)
        return s if len(s) <= max_width else s[: max_width - 1] + "…"

    body = [[cell(v) for v in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in body)) if body else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c.ljust(w) for c, w in zip(r, widths)) for r in body]
    return "\n".join(out)


def _load(args: argparse.Namespace, *, logs: bool = True) -> PipelineConfig:
    cfg = load_config(args.config)
    setup_logging(cfg.logs_dir if logs else None, args.log_level)
    return cfg


# ----------------------------------------------------------------------- commands


def cmd_validate(args: argparse.Namespace) -> int:
    from .layers.gold import discover_models

    cfg = _load(args, logs=False)
    print(f"config OK - project root: {cfg.root}")
    for s in cfg.sources:
        rules = len(s.quality)
        print(f"  source {s.name:<18} {s.type:<9} {s.load_mode:<12} {len(s.columns)} columns, {rules} quality rules")
    models = discover_models(cfg.gold_sql_dir)
    print(f"  gold models ({len(models)}): " + ", ".join(m.name for m in models))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    from .runner import Runner

    cfg = _load(args)
    with Runner(cfg) as runner:
        summary = runner.run(
            args.source,
            skip_gold=args.skip_gold,
            gold_only=args.gold_only,
            full_refresh=args.full_refresh,
            trigger="cli",
        )
    print()
    print(f"run {summary.run_id}: {summary.status.upper()} in {summary.seconds:.1f}s   gold: {summary.gold}")
    rows = [(o.source, o.status, o.rows_in, o.rows_out, o.quarantined, o.message) for o in summary.sources]
    if rows:
        print(_table(("source", "status", "rows_in", "rows_out", "quarantined", "message"), rows, 70))
    return 0 if summary.ok else 1


def cmd_reprocess(args: argparse.Namespace) -> int:
    from .runner import Runner

    cfg = _load(args)
    with Runner(cfg) as runner:
        summary = runner.reprocess(args.source, args.run_id)
    for o in summary.sources:
        print(f"{o.source}: {o.status} in={o.rows_in} out={o.rows_out} quarantined={o.quarantined} {o.message}")
    return 0 if summary.ok else 1


def cmd_status(args: argparse.Namespace) -> int:
    from .warehouse import Warehouse

    cfg = _load(args, logs=False)
    wh = Warehouse(cfg)
    try:
        runs = wh.execute(
            "SELECT run_id, trigger, status, started_at, "
            "date_diff('second', started_at, finished_at) AS secs FROM meta.pipeline_runs "
            "ORDER BY started_at DESC LIMIT ?", [args.limit]
        ).fetchall()
        print("Recent runs")
        print(_table(("run_id", "trigger", "status", "started_at (UTC)", "secs"), runs) if runs else "  (none yet)")
        stages = wh.execute(
            "SELECT source, stage, status, rows_in, rows_out, rows_quarantined, rows_warn, message "
            "FROM meta.stage_runs WHERE run_id = (SELECT run_id FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 1) "
            "ORDER BY started_at"
        ).fetchall()
        if stages:
            print("\nLatest run, by stage")
            print(_table(("source", "stage", "status", "in", "out", "quarantined", "warn", "message"), stages, 50))
        wms = wh.execute("SELECT source, watermark, updated_at FROM meta.watermarks ORDER BY source").fetchall()
        if wms:
            print("\nWatermarks")
            print(_table(("source", "watermark", "updated_at"), wms))
    finally:
        wh.close()
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    from .warehouse import Warehouse

    cfg = _load(args, logs=False)
    wh = Warehouse(cfg)
    try:
        where, params = "", []
        if args.source:
            where, params = "WHERE source = ?", [args.source]
        rows = wh.execute(
            f"""SELECT source, rule, severity, violations, outcome FROM meta.quality_results {where}
                QUALIFY run_id = first_value(run_id) OVER (PARTITION BY source ORDER BY checked_at DESC)
                ORDER BY source, outcome = 'pass', rule""", params
        ).fetchall()
        print(_table(("source", "rule", "severity", "violations", "outcome"), rows) if rows else "no quality results yet")
    finally:
        wh.close()
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    from .warehouse import Warehouse

    cfg = _load(args, logs=False)
    wh = Warehouse(cfg)
    try:
        wh.refresh_all_views()
        cur = wh.execute(args.sql)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print(_table(headers, rows, 40))
        print(f"\n({len(rows)} rows)")
    finally:
        wh.close()
    return 0


def cmd_init_demo(args: argparse.Namespace) -> int:
    from .demo.generate import generate_demo

    cfg = _load(args, logs=False)
    for line in generate_demo(cfg.root, new_orders=args.append_orders, seed=args.seed):
        print(line)
    return 0


def cmd_mock_api(args: argparse.Namespace) -> int:
    from .demo.mock_api import serve

    serve(args.host, args.port)
    return 0


# --------------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline", description="Config-driven data pipeline (bronze/silver/gold).")
    p.add_argument("-c", "--config", default=DEFAULT_CONFIG, help=f"config file (default {DEFAULT_CONFIG})")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("validate", help="check config + gold models without running anything").set_defaults(func=cmd_validate)

    r = sub.add_parser("run", help="extract -> bronze -> silver -> gold")
    r.add_argument("-s", "--source", action="append", help="only this source (repeatable)")
    r.add_argument("--skip-gold", action="store_true", help="stop after silver")
    r.add_argument("--gold-only", action="store_true", help="only rebuild gold models")
    r.add_argument("--full-refresh", action="store_true", help="ignore watermarks/fingerprints, replace silver")
    r.set_defaults(func=cmd_run)

    rp = sub.add_parser("reprocess", help="rebuild silver of one source from bronze (after changing rules)")
    rp.add_argument("source")
    rp.add_argument("--run-id", action="append", help="bronze run id(s) to use (default: automatic)")
    rp.set_defaults(func=cmd_reprocess)

    st = sub.add_parser("status", help="recent runs, per-stage metrics, watermarks")
    st.add_argument("-n", "--limit", type=int, default=10)
    st.set_defaults(func=cmd_status)

    qa = sub.add_parser("quality", help="latest data-quality results")
    qa.add_argument("-s", "--source")
    qa.set_defaults(func=cmd_quality)

    qu = sub.add_parser("query", help="run SQL against the warehouse (schemas: silver, gold, meta)")
    qu.add_argument("sql")
    qu.set_defaults(func=cmd_query)

    d = sub.add_parser("init-demo", help="create demo inputs (sqlite shop.db, .env); --append-orders simulates new data")
    d.add_argument("--append-orders", type=int, default=0, metavar="N", help="add N new orders + update some old ones")
    d.add_argument("--seed", type=int, default=42)
    d.set_defaults(func=cmd_init_demo)

    m = sub.add_parser("mock-api", help="serve the demo stock-movements API")
    m.add_argument("--host", default="127.0.0.1")
    m.add_argument("--port", type=int, default=8000)
    m.set_defaults(func=cmd_mock_api)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
