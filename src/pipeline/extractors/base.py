"""Extractor contract.

An extractor reads ONE source and writes ONE utf-8 CSV file into the landing zone
(data/landing/<source>/<run_id>/<source>.csv). Landing is deliberately dumb: no typing, no cleaning.
Bronze then loads that file as-is (all columns VARCHAR) so every later stage can be replayed.
"""

from __future__ import annotations

import csv
import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from ..config import SourceConfig, expand_env


@dataclass
class ExtractResult:
    path: Path | None = None
    rows: int = 0
    new_watermark: str | None = None  # committed only after silver succeeds
    fingerprint: str | None = None  # committed only after silver succeeds
    skipped: bool = False
    reason: str = ""


class Extractor(ABC):
    def __init__(self, source: SourceConfig, root: Path):
        self.source = source
        self.root = root
        # secrets / env substitution is lazy so unrelated sources never fail validation
        self.options: dict[str, Any] = expand_env(source.options)

    @abstractmethod
    def extract(self, out_dir: Path, *, watermark: str | None, fingerprint: str | None) -> ExtractResult:
        """Write the landing file into out_dir and describe what happened."""

    def target(self, out_dir: Path) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"{self.source.name}.csv"

    def resolve(self, path: str) -> Path:
        p = Path(path)
        return p if p.is_absolute() else (self.root / p)


def csv_cell(value: Any) -> str:
    """Render any Python value as a CSV cell (None -> empty -> NULL in bronze)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (dt.date, dt.time, Decimal)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def write_csv(path: Path, header: list[str], rows: Iterable[Iterable[Any]]) -> int:
    n = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for row in rows:
            w.writerow([csv_cell(v) for v in row])
            n += 1
    return n
