"""Silver: typed, validated, de-duplicated, one row per primary key.

Steps (all in DuckDB, nothing is published until every dataset-level check has passed):
  1. read bronze rows of the requested runs
  2. rename + trim + cast to the declared types (failed casts become violations, never silent NULLs)
  3. evaluate row-level quality rules  -> error: quarantine the row   | warn: keep, count
  4. de-duplicate on the primary key (latest ingested row wins)
  5. merge into the existing silver table (incremental upsert) or replace it (full load)
  6. evaluate dataset-level rules on the candidate table; any failed `error` rule aborts the run
  7. atomically publish data/silver/<source>.parquet and refresh the silver.<source> view
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..config import ColumnSpec, PipelineConfig, QualityRule, SourceConfig
from ..errors import DataQualityError, SchemaError
from ..sqlutil import lit, path_lit, q
from ..warehouse import QualityOutcome, Warehouse
from .bronze import bronze_glob

log = logging.getLogger("pipeline.silver")

LINEAGE = ("_run_id", "_ingested_at", "_row_hash")
_EMPTY_LIST = "CAST([] AS VARCHAR[])"


@dataclass
class SilverResult:
    rows_in: int = 0
    rows_out: int = 0  # valid, de-duplicated rows produced by this batch
    quarantined: int = 0
    warn_rows: int = 0
    duplicates_dropped: int = 0
    silver_total: int = 0
    outcomes: list[QualityOutcome] = field(default_factory=list)


# ---------------------------------------------------------------- SQL builders


def _typed_expr(col: ColumnSpec) -> tuple[str, str | None]:
    """Return (typed expression, cast-failure predicate or None) for a column of alias b."""
    raw = f"b.{q(col.source)}"
    cleaned = f"NULLIF(TRIM({raw}), '')" if col.trim else f"NULLIF({raw}, '')"
    base = col.base_type
    if base == "VARCHAR":
        return cleaned, None
    if base == "BOOLEAN":
        expr = (
            f"CASE WHEN lower({cleaned}) IN ('true','t','yes','y','1') THEN TRUE "
            f"WHEN lower({cleaned}) IN ('false','f','no','n','0') THEN FALSE END"
        )
    elif col.format and base in {"DATE", "TIMESTAMP", "TIME"}:
        expr = f"CAST(try_strptime({cleaned}, {lit(col.format)}) AS {col.type})"
    else:
        expr = f"TRY_CAST({cleaned} AS {col.type})"
    return expr, f"({cleaned} IS NOT NULL AND ({expr}) IS NULL)"


def _violation_expr(rule: QualityRule) -> str:
    """SQL predicate that is TRUE when a row VIOLATES the rule (evaluated on typed columns)."""
    p = rule.params
    c = q(rule.column) if rule.column else None
    if rule.rule == "not_null":
        return f"{c} IS NULL"
    if rule.rule == "range":
        parts = []
        if p.get("min") is not None:
            parts.append(f"{c} < {lit(p['min'])}")
        if p.get("max") is not None:
            parts.append(f"{c} > {lit(p['max'])}")
        return f"{c} IS NOT NULL AND ({' OR '.join(parts)})"
    if rule.rule == "accepted_values":
        values = ", ".join(lit(str(v)) for v in p["values"])
        return f"{c} IS NOT NULL AND CAST({c} AS VARCHAR) NOT IN ({values})"
    if rule.rule == "regex":
        return f"{c} IS NOT NULL AND NOT regexp_matches(CAST({c} AS VARCHAR), {lit(p['pattern'])})"
    if rule.rule == "expression":  # the expression states what must be TRUE
        return f"NOT COALESCE(({p['expr']}), TRUE)"
    raise AssertionError(rule.rule)


def _names_list(names: list[str], flags: list[str]) -> str:
    if not names:
        return _EMPTY_LIST
    cases = ", ".join(f"CASE WHEN {f} THEN {lit(n)} END" for n, f in zip(names, flags))
    return f"list_filter([{cases}], lambda x: x IS NOT NULL)"


# ------------------------------------------------------------------- main entry


def build_silver(
    wh: Warehouse,
    cfg: PipelineConfig,
    source: SourceConfig,
    run_id: str,
    bronze_run_ids: list[str],
    *,
    replace: bool,
) -> SilverResult:
    name = source.name
    glob = bronze_glob(cfg, name)
    if not bronze_run_ids:
        raise SchemaError(f"[{name}] no bronze data to process")
    run_filter = ", ".join(lit(r) for r in bronze_run_ids)

    wh.execute(
        f"CREATE OR REPLACE TEMP VIEW _bronze AS SELECT * FROM read_parquet({path_lit(glob)}, union_by_name = true, hive_partitioning = false) "
        f"WHERE _run_id IN ({run_filter})"
    )

    # ---- schema contract: every declared source column must exist --------------------------------
    present = {r[0].lower(): r[0] for r in wh.execute("DESCRIBE SELECT * FROM _bronze").fetchall()}
    missing = [c.source for c in source.columns if c.source.lower() not in present]
    if missing:
        raise SchemaError(f"[{name}] declared column(s) missing from source data: {missing}")
    declared = {c.source.lower() for c in source.columns}
    extra = [orig for low, orig in present.items() if low not in declared and not low.startswith("_")]
    if extra:
        log.warning("[%s] schema drift: undeclared source column(s) ignored: %s", name, extra)

    # ---- 1. typed ---------------------------------------------------------------------------------
    select_cols, cast_names, cast_flags = [], [], []
    for col in source.columns:
        expr, fail = _typed_expr(col)
        select_cols.append(f"{expr} AS {q(col.name)}")
        if fail:
            cast_names.append(f"type_cast:{col.name}")
            cast_flags.append(fail)
    bad_cast = _names_list(cast_names, cast_flags)
    wh.execute(
        f"""CREATE OR REPLACE TEMP TABLE _typed AS
            SELECT {', '.join(select_cols)},
                   b._run_id, b._ingested_at, b._row_number, b._row_hash,
                   CAST(to_json(b) AS VARCHAR) AS _raw_record,
                   {bad_cast} AS _bad_cast
            FROM _bronze b"""
    )
    rows_in = wh.execute("SELECT count(*) FROM _typed").fetchone()[0]

    # ---- 2. row-level rules -------------------------------------------------------------------------
    rules = source.row_rules()
    flag_cols = [f"_v{i}" for i in range(len(rules))]
    flag_sql = ", ".join(f"COALESCE(({_violation_expr(r)}), FALSE) AS {f}" for r, f in zip(rules, flag_cols))
    wh.execute(f"CREATE OR REPLACE TEMP TABLE _flags AS SELECT *{', ' + flag_sql if flag_sql else ''} FROM _typed")

    err = [(r, f) for r, f in zip(rules, flag_cols) if r.severity == "error"]
    wrn = [(r, f) for r, f in zip(rules, flag_cols) if r.severity == "warn"]
    err_list = _names_list([r.name for r, _ in err], [f for _, f in err])
    wrn_list = _names_list([r.name for r, _ in wrn], [f for _, f in wrn])
    wh.execute(
        f"""CREATE OR REPLACE TEMP TABLE _checked AS
            SELECT *, list_concat(_bad_cast, {err_list}) AS _violations, {wrn_list} AS _warnings
            FROM _flags"""
    )

    res = SilverResult(rows_in=rows_in)
    if rules:
        agg = ", ".join(f"count(*) FILTER (WHERE {f}) AS {f}" for f in flag_cols)
        counts = dict(zip(flag_cols, wh.execute(f"SELECT {agg} FROM _flags").fetchone()))
    else:
        counts = {}
    for r, f in zip(rules, flag_cols):
        n = counts[f]
        outcome = "pass" if n == 0 else ("warn" if r.severity == "warn" else "quarantined")
        res.outcomes.append(QualityOutcome(name, r.name, r.severity, r.column, n, outcome))
    for cast_name, n in wh.execute(
        "SELECT r, count(*) FROM (SELECT unnest(_bad_cast) AS r FROM _typed) GROUP BY r"
    ).fetchall():
        res.outcomes.append(
            QualityOutcome(name, cast_name, "error", cast_name.split(":", 1)[1], n, "quarantined")
        )
    for cn in cast_names:
        if not any(o.rule == cn for o in res.outcomes):
            res.outcomes.append(QualityOutcome(name, cn, "error", cn.split(":", 1)[1], 0, "pass"))

    # ---- 3. quarantine ---------------------------------------------------------------------------
    res.quarantined = wh.execute("SELECT count(*) FROM _checked WHERE len(_violations) > 0").fetchone()[0]
    if res.quarantined:
        qdir = cfg.quarantine_dir / name
        qdir.mkdir(parents=True, exist_ok=True)
        qpath = qdir / f"run_id={run_id}.parquet"
        wh.execute(
            f"""COPY (SELECT _run_id, _row_number, array_to_string(_violations, '; ') AS _reasons, _raw_record
                      FROM _checked WHERE len(_violations) > 0 ORDER BY _row_number)
                TO {path_lit(qpath)} (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
        log.warning("[%s] %d/%d row(s) quarantined -> %s", name, res.quarantined, rows_in, qpath.relative_to(cfg.root))

    # ---- 4. de-duplicate -------------------------------------------------------------------------
    typed_names = ", ".join(q(c.name) for c in source.columns)
    pk = ", ".join(q(k) for k in source.primary_key)
    keep = f"{typed_names}, {', '.join(LINEAGE)}"
    wh.execute(
        f"""CREATE OR REPLACE TEMP TABLE _deduped AS
            SELECT {keep}, len(_warnings) > 0 AS _has_warning FROM (
                SELECT *, row_number() OVER (PARTITION BY {pk} ORDER BY _ingested_at DESC, _row_number DESC) AS _rn
                FROM _checked WHERE len(_violations) = 0
            ) WHERE _rn = 1"""
    )
    res.rows_out = wh.execute("SELECT count(*) FROM _deduped").fetchone()[0]
    res.warn_rows = wh.execute("SELECT count(*) FROM _deduped WHERE _has_warning").fetchone()[0]
    res.duplicates_dropped = rows_in - res.quarantined - res.rows_out
    if res.duplicates_dropped:
        log.warning("[%s] %d duplicate primary-key row(s) dropped (latest kept)", name, res.duplicates_dropped)

    # ---- 5. candidate table ---------------------------------------------------------------------
    silver_path = cfg.silver_dir / f"{name}.parquet"
    prev_total: int | None = None
    if silver_path.exists():
        prev_total = wh.execute(f"SELECT count(*) FROM read_parquet({path_lit(silver_path)})").fetchone()[0]
    if replace or prev_total is None:
        wh.execute(f"CREATE OR REPLACE TEMP TABLE _candidate AS SELECT {keep} FROM _deduped")
    else:
        on = " AND ".join(f"e.{q(k)} IS NOT DISTINCT FROM n.{q(k)}" for k in source.primary_key)
        wh.execute(
            f"""CREATE OR REPLACE TEMP TABLE _candidate AS
                SELECT e.* FROM read_parquet({path_lit(silver_path)}) e
                WHERE NOT EXISTS (SELECT 1 FROM _deduped n WHERE {on})
                UNION ALL BY NAME
                SELECT {keep} FROM _deduped"""
        )
    res.silver_total = wh.execute("SELECT count(*) FROM _candidate").fetchone()[0]

    # ---- 6. dataset-level rules -------------------------------------------------------------------
    failures: list[str] = []
    for r in source.dataset_rules():
        violated, detail = _check_dataset_rule(wh, r, res, rows_in, prev_total)
        outcome = "pass" if not violated else ("warn" if r.severity == "warn" else "fail")
        res.outcomes.append(QualityOutcome(name, r.name, r.severity, r.column, 1 if violated else 0, outcome))
        if violated:
            log.log(30 if r.severity == "warn" else 40, "[%s] dataset rule '%s' violated: %s", name, r.name, detail)
            if r.severity == "error":
                failures.append(f"{r.name}: {detail}")
    wh.record_quality(run_id, res.outcomes)
    if failures:
        raise DataQualityError(f"[{name}] silver NOT published. " + " | ".join(failures))

    # ---- 7. publish --------------------------------------------------------------------------------
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = silver_path.with_suffix(".parquet.tmp")
    wh.execute(f"COPY _candidate TO {path_lit(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp.replace(silver_path)
    wh.refresh_silver_view(name)
    for tbl in ("_bronze", "_typed", "_flags", "_checked", "_deduped", "_candidate"):
        wh.execute(f"DROP VIEW IF EXISTS {tbl}" if tbl == "_bronze" else f"DROP TABLE IF EXISTS {tbl}")
    log.info(
        "[%s] silver: in=%d out=%d quarantined=%d warn=%d dup=%d total=%d",
        name, rows_in, res.rows_out, res.quarantined, res.warn_rows, res.duplicates_dropped, res.silver_total,
    )
    return res


def _check_dataset_rule(
    wh: Warehouse, r: QualityRule, res: SilverResult, rows_in: int, prev_total: int | None
) -> tuple[bool, str]:
    p: dict[str, Any] = r.params
    if r.rule == "min_rows":
        need = int(p["value"])
        return res.silver_total < need, f"{res.silver_total} rows < required {need}"
    if r.rule == "max_quarantine_ratio":
        limit = float(p["value"])
        ratio = (res.quarantined / rows_in) if rows_in else 0.0
        return ratio > limit, f"{ratio:.1%} of batch quarantined (> {limit:.1%})"
    if r.rule == "max_row_drop_pct":
        if not prev_total:
            return False, ""
        drop = (prev_total - res.silver_total) / prev_total * 100
        return drop > float(p["value"]), f"row count fell {drop:.1f}% ({prev_total} -> {res.silver_total})"
    if r.rule == "freshness":
        col = q(r.column)
        age = wh.execute(
            f"SELECT date_diff('day', CAST(max({col}) AS TIMESTAMP), CAST(current_timestamp AS TIMESTAMP)) "
            f"FROM _candidate"
        ).fetchone()[0]
        if age is None:
            return True, f"no values in {r.column}"
        return age > float(p["max_age_days"]), f"newest {r.column} is {age} day(s) old (> {p['max_age_days']})"
    raise AssertionError(r.rule)
