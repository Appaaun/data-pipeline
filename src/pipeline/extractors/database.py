"""Relational database source via SQLAlchemy (SQLite, PostgreSQL, MySQL, SQL Server ...).

Incremental loads use a watermark column: only rows with column > last_watermark are pulled.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from ..errors import ConfigError, ExtractError
from .base import ExtractResult, Extractor, csv_cell, write_csv

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _coerce_watermark(value: str, kind: str) -> Any:
    if kind == "int":
        return int(value)
    if kind == "float":
        return float(value)
    return value


class DatabaseExtractor(Extractor):
    """options:
        url:        SQLAlchemy URL, e.g. postgresql+psycopg://user:${DB_PASSWORD}@host/db
                    (relative sqlite paths are resolved against the project root)
        table:      table name (use this OR query)
        query:      custom SELECT. For incremental loads it must contain :watermark
        chunk_size: rows fetched per round trip (default 10000)
    incremental:
        column:         watermark column (monotonic: updated_at, id ...)
        initial_value:  value used on the first run (default "1970-01-01 00:00:00")
        watermark_type: string | int | float (default string)
    """

    def _url(self) -> str:
        url = self.options.get("url")
        if not url:
            raise ExtractError(f"[{self.source.name}] database source needs options.url")
        parsed = make_url(url)
        if parsed.drivername.startswith("sqlite") and parsed.database and parsed.database != ":memory:":
            db_path = Path(parsed.database)
            if not db_path.is_absolute():
                parsed = parsed.set(database=str((self.root / db_path).resolve()))
        return parsed.render_as_string(hide_password=False)

    def _statement(self, incremental_col: str | None) -> str:
        table, query = self.options.get("table"), self.options.get("query")
        if bool(table) == bool(query):
            raise ConfigError(f"[{self.source.name}] set exactly one of options.table / options.query")
        if table:
            if not _TABLE_RE.match(table):
                raise ConfigError(f"[{self.source.name}] invalid table name {table!r}")
            if incremental_col:
                if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", incremental_col):
                    raise ConfigError(f"[{self.source.name}] invalid incremental.column {incremental_col!r}")
                return (
                    f"SELECT * FROM {table} WHERE {incremental_col} > :watermark ORDER BY {incremental_col}"
                )
            return f"SELECT * FROM {table}"
        if incremental_col and ":watermark" not in query:
            raise ConfigError(f"[{self.source.name}] incremental query must reference :watermark")
        return query

    def extract(self, out_dir: Path, *, watermark: str | None, fingerprint: str | None) -> ExtractResult:
        inc = self.source.incremental
        inc_col = inc.get("column") if self.source.is_incremental else None
        kind = inc.get("watermark_type", "string")
        stmt = self._statement(inc_col)
        params: dict[str, Any] = {}
        if inc_col:
            start = watermark if watermark is not None else str(inc.get("initial_value", "1970-01-01 00:00:00"))
            params["watermark"] = _coerce_watermark(start, kind)

        engine = create_engine(self._url())
        chunk = int(self.options.get("chunk_size", 10_000))
        dest = self.target(out_dir)
        max_seen: Any = None
        count = 0
        try:
            with engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(text(stmt), params)
                header = list(result.keys())
                inc_idx = header.index(inc_col) if inc_col and inc_col in header else None
                if inc_col and inc_idx is None:
                    raise ExtractError(
                        f"[{self.source.name}] watermark column '{inc_col}' is not in the query result {header}"
                    )

                def rows() -> Iterator[tuple]:
                    nonlocal max_seen
                    while True:
                        batch = result.fetchmany(chunk)
                        if not batch:
                            return
                        for row in batch:
                            if inc_idx is not None and row[inc_idx] is not None:
                                v = row[inc_idx]
                                if max_seen is None or v > max_seen:
                                    max_seen = v
                            yield tuple(row)

                count = write_csv(dest, header, rows())
        except SQLAlchemyError as exc:
            raise ExtractError(f"[{self.source.name}] database error: {exc.__class__.__name__}: {exc}") from exc
        finally:
            engine.dispose()

        if count == 0:
            dest.unlink(missing_ok=True)
            return ExtractResult(skipped=True, reason="no new rows since watermark")
        new_wm = csv_cell(max_seen) if max_seen is not None else None
        return ExtractResult(path=dest, rows=count, new_watermark=new_wm)
