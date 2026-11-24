"""Configuration loading and validation.

The whole pipeline is described by one YAML file (see config/pipeline.yaml).
Validation is strict and happens up-front so that typos fail at `pipeline validate`,
not in the middle of a run.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigError

IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
TYPE_RE = re.compile(r"^([A-Za-z]+)(\(\s*\d+\s*(,\s*\d+\s*)?\))?$")
ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

ALLOWED_TYPES = {
    "VARCHAR", "INTEGER", "BIGINT", "SMALLINT", "DOUBLE", "FLOAT", "DECIMAL",
    "DATE", "TIMESTAMP", "TIME", "BOOLEAN",
}
SOURCE_TYPES = {"csv", "api", "database"}
LOAD_MODES = {"full", "incremental"}
SEVERITIES = {"error", "warn"}

# row-level rules: violating rows are quarantined (error) or just counted (warn)
ROW_RULES = {"not_null", "range", "accepted_values", "regex", "expression"}
# dataset-level rules: evaluated on the batch / resulting silver table
DATASET_RULES = {"min_rows", "max_quarantine_ratio", "freshness", "max_row_drop_pct"}

_RULE_REQUIRED: dict[str, tuple[str, ...]] = {
    "not_null": ("column",),
    "range": ("column",),
    "accepted_values": ("column", "values"),
    "regex": ("column", "pattern"),
    "expression": ("expr",),
    "min_rows": ("value",),
    "max_quarantine_ratio": ("value",),
    "freshness": ("column", "max_age_days"),
    "max_row_drop_pct": ("value",),
}


# --------------------------------------------------------------------------- env


def load_dotenv(path: Path) -> None:
    """Minimal .env loader. Existing environment variables win."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


def expand_env(value: Any) -> Any:
    """Recursively expand ${VAR} / ${VAR:-default} in strings. Missing vars raise ConfigError."""
    if isinstance(value, str):

        def repl(m: re.Match[str]) -> str:
            name, default = m.group(1), m.group(2)
            found = os.environ.get(name)
            if found is not None:
                return found
            if default is not None:
                return default
            raise ConfigError(f"environment variable {name} is not set (needed by config)")

        return ENV_RE.sub(repl, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


# ------------------------------------------------------------------------ models


@dataclass(frozen=True)
class ColumnSpec:
    name: str  # silver column name (snake_case)
    source: str  # column name as it appears in the source / bronze
    type: str = "VARCHAR"
    nullable: bool = True
    trim: bool = True
    format: str | None = None  # strptime format for DATE/TIMESTAMP parsing

    @property
    def base_type(self) -> str:
        return self.type.split("(")[0].upper()


@dataclass(frozen=True)
class QualityRule:
    rule: str
    name: str
    severity: str = "error"
    column: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def is_row_level(self) -> bool:
        return self.rule in ROW_RULES


@dataclass(frozen=True)
class SourceConfig:
    name: str
    type: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    load_mode: str = "full"
    incremental: dict[str, Any] = field(default_factory=dict)
    quality: tuple[QualityRule, ...] = ()
    options: dict[str, Any] = field(default_factory=dict)  # type-specific (path/url/query...)
    description: str = ""

    @property
    def is_incremental(self) -> bool:
        return self.load_mode == "incremental"

    def column(self, name: str) -> ColumnSpec:
        for c in self.columns:
            if c.name == name:
                return c
        raise ConfigError(f"[{self.name}] unknown column '{name}'")

    def row_rules(self) -> list[QualityRule]:
        return [r for r in self.quality if r.is_row_level]

    def dataset_rules(self) -> list[QualityRule]:
        return [r for r in self.quality if not r.is_row_level]


@dataclass(frozen=True)
class PipelineConfig:
    root: Path
    data_dir: Path
    warehouse_path: Path
    gold_sql_dir: Path
    logs_dir: Path
    landing_retention_days: int
    sources: tuple[SourceConfig, ...]

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    def source(self, name: str) -> SourceConfig:
        for s in self.sources:
            if s.name == name:
                return s
        known = ", ".join(s.name for s in self.sources)
        raise ConfigError(f"unknown source '{name}' (known: {known})")

    def select(self, names: list[str] | None) -> list[SourceConfig]:
        if not names:
            return list(self.sources)
        return [self.source(n) for n in names]


# ---------------------------------------------------------------------- parsing


def _require_ident(value: Any, what: str) -> str:
    if not isinstance(value, str) or not IDENT_RE.match(value):
        raise ConfigError(f"{what} must match [A-Za-z][A-Za-z0-9_]* (got {value!r})")
    return value


def _parse_column(src: str, raw: dict[str, Any]) -> ColumnSpec:
    if not isinstance(raw, dict) or "name" not in raw:
        raise ConfigError(f"[{src}] every column needs at least a 'name': {raw!r}")
    name = _require_ident(raw["name"], f"[{src}] column name")
    col_type = str(raw.get("type", "VARCHAR")).strip()
    m = TYPE_RE.match(col_type)
    if not m or m.group(1).upper() not in ALLOWED_TYPES:
        raise ConfigError(
            f"[{src}.{name}] unsupported type '{col_type}' (allowed: {', '.join(sorted(ALLOWED_TYPES))})"
        )
    return ColumnSpec(
        name=name,
        source=str(raw.get("source", name)),
        type=col_type.upper().replace(" ", ""),
        nullable=bool(raw.get("nullable", True)),
        trim=bool(raw.get("trim", True)),
        format=raw.get("format"),
    )


def _parse_rule(src: str, raw: dict[str, Any], index: int) -> QualityRule:
    if not isinstance(raw, dict) or "rule" not in raw:
        raise ConfigError(f"[{src}] quality rule #{index} needs a 'rule' key: {raw!r}")
    rule = raw["rule"]
    if rule not in ROW_RULES | DATASET_RULES:
        raise ConfigError(
            f"[{src}] unknown rule '{rule}' (allowed: {', '.join(sorted(ROW_RULES | DATASET_RULES))})"
        )
    severity = raw.get("severity", "error")
    if severity not in SEVERITIES:
        raise ConfigError(f"[{src}] rule '{rule}': severity must be one of {sorted(SEVERITIES)}")
    for req in _RULE_REQUIRED[rule]:
        if raw.get(req) is None:
            raise ConfigError(f"[{src}] rule '{rule}' requires '{req}'")
    if rule == "range" and raw.get("min") is None and raw.get("max") is None:
        raise ConfigError(f"[{src}] rule 'range' on '{raw['column']}' needs min and/or max")
    if rule == "accepted_values" and not isinstance(raw["values"], list):
        raise ConfigError(f"[{src}] rule 'accepted_values' needs a list under 'values'")
    if rule == "max_quarantine_ratio" and not 0 <= float(raw["value"]) <= 1:
        raise ConfigError(f"[{src}] max_quarantine_ratio must be between 0 and 1")

    column = raw.get("column")
    if raw.get("name"):
        name = str(raw["name"])
    elif rule == "expression":
        name = f"expression:{str(raw['expr']).strip()[:40]}"
    elif column:
        name = f"{rule}:{column}"
    else:
        name = rule
    params = {k: v for k, v in raw.items() if k not in {"rule", "name", "severity", "column"}}
    return QualityRule(rule=rule, name=name, severity=severity, column=column, params=params)


def _parse_source(raw: dict[str, Any]) -> SourceConfig:
    name = _require_ident(raw.get("name"), "source name")
    stype = raw.get("type")
    if stype not in SOURCE_TYPES:
        raise ConfigError(f"[{name}] type must be one of {sorted(SOURCE_TYPES)} (got {stype!r})")
    load_mode = raw.get("load_mode", "full")
    if load_mode not in LOAD_MODES:
        raise ConfigError(f"[{name}] load_mode must be one of {sorted(LOAD_MODES)}")
    if stype == "csv" and load_mode != "full":
        raise ConfigError(f"[{name}] csv sources only support load_mode: full")

    columns = tuple(_parse_column(name, c) for c in raw.get("columns") or [])
    if not columns:
        raise ConfigError(f"[{name}] needs a non-empty 'columns' list")
    names = [c.name for c in columns]
    if len(set(names)) != len(names):
        raise ConfigError(f"[{name}] duplicate column names")

    pk = tuple(raw.get("primary_key") or [])
    if not pk:
        raise ConfigError(f"[{name}] needs a primary_key (used for de-duplication and upserts)")
    for k in pk:
        if k not in names:
            raise ConfigError(f"[{name}] primary_key column '{k}' is not in columns")

    incremental = dict(raw.get("incremental") or {})
    if load_mode == "incremental" and not incremental.get("column") and not incremental.get("param"):
        raise ConfigError(f"[{name}] incremental sources need incremental.column (database) or incremental.param (api)")

    rules = [_parse_rule(name, r, i) for i, r in enumerate(raw.get("quality") or [])]
    for r in rules:
        if r.column and r.is_row_level and r.column not in names:
            raise ConfigError(f"[{name}] rule '{r.name}' references unknown column '{r.column}'")
        if r.rule == "freshness" and r.column not in names:
            raise ConfigError(f"[{name}] rule '{r.name}' references unknown column '{r.column}'")

    # implicit rules: non-nullable columns and primary-key columns must not be null
    have = {(r.rule, r.column) for r in rules}
    implicit: list[QualityRule] = []
    for c in columns:
        if (not c.nullable or c.name in pk) and ("not_null", c.name) not in have:
            implicit.append(QualityRule("not_null", f"not_null:{c.name}", "error", c.name, {}))
    rules = implicit + rules

    seen: dict[str, int] = {}
    uniq: list[QualityRule] = []
    for r in rules:  # rule names must be unique: they identify flags and reports
        n = seen.get(r.name, 0) + 1
        seen[r.name] = n
        uniq.append(r if n == 1 else QualityRule(r.rule, f"{r.name}#{n}", r.severity, r.column, r.params))

    return SourceConfig(
        name=name,
        type=stype,
        columns=columns,
        primary_key=pk,
        load_mode=load_mode,
        incremental=incremental,
        quality=tuple(uniq),
        options=dict(raw.get("options") or {}),
        description=str(raw.get("description", "")),
    )


def config_from_dict(raw: dict[str, Any], root: Path) -> PipelineConfig:
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a mapping")
    project = raw.get("project") or {}
    root = root.resolve()

    def p(key: str, default: str) -> Path:
        return (root / str(project.get(key, default))).resolve()

    sources = tuple(_parse_source(s) for s in raw.get("sources") or [])
    if not sources:
        raise ConfigError("config has no sources")
    names = [s.name for s in sources]
    if len(set(names)) != len(names):
        raise ConfigError("duplicate source names")

    return PipelineConfig(
        root=root,
        data_dir=p("data_dir", "data"),
        warehouse_path=p("warehouse", "data/warehouse/warehouse.duckdb"),
        gold_sql_dir=p("gold_sql_dir", "sql/gold"),
        logs_dir=p("logs_dir", "logs"),
        landing_retention_days=int(project.get("landing_retention_days", 7)),
        sources=sources,
    )


def load_config(path: str | Path) -> PipelineConfig:
    """Load config/pipeline.yaml. Project root = the folder above the config's folder."""
    path = Path(path).resolve()
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    root = path.parent.parent
    load_dotenv(root / ".env")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    return config_from_dict(raw, root)
