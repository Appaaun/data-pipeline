"""Tiny helpers for building SQL strings safely (identifiers and literals)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any


def q(identifier: str) -> str:
    """Quote an identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def lit(value: Any) -> str:
    """Render a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return repr(value) if not isinstance(value, Decimal) else str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return "'" + value.isoformat() + "'"
    return "'" + str(value).replace("'", "''") + "'"


def path_lit(path: Path | str) -> str:
    """Render a filesystem path as a SQL string literal (forward slashes, works on Windows)."""
    return lit(Path(path).as_posix())


def str_list(values: list[str]) -> str:
    return "[" + ", ".join(lit(v) for v in values) + "]"
