from __future__ import annotations

from pathlib import Path

from ..config import SourceConfig
from .base import ExtractResult, Extractor
from .csv_file import CsvExtractor
from .database import DatabaseExtractor
from .rest_api import ApiExtractor

EXTRACTORS: dict[str, type[Extractor]] = {
    "csv": CsvExtractor,
    "api": ApiExtractor,
    "database": DatabaseExtractor,
}


def build_extractor(source: SourceConfig, root: Path) -> Extractor:
    return EXTRACTORS[source.type](source, root)


__all__ = ["ExtractResult", "Extractor", "build_extractor", "EXTRACTORS"]
