"""CSV file source (full load). Skips the run when the file content has not changed."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from ..errors import ExtractError
from .base import ExtractResult, Extractor


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class CsvExtractor(Extractor):
    """options:
        path:      file path (relative to project root)
        encoding:  source encoding, default utf-8 (use cp874 / tis-620 for old Thai files)
        delimiter: default ","
    """

    def extract(self, out_dir: Path, *, watermark: str | None, fingerprint: str | None) -> ExtractResult:
        if "path" not in self.options:
            raise ExtractError(f"[{self.source.name}] csv source needs options.path")
        src = self.resolve(self.options["path"])
        if not src.is_file():
            raise ExtractError(f"[{self.source.name}] file not found: {src}")

        fp = sha256_file(src)
        if fingerprint is not None and fp == fingerprint:
            return ExtractResult(skipped=True, reason="file unchanged since last successful run", fingerprint=fp)

        dest = self.target(out_dir)
        encoding = str(self.options.get("encoding", "utf-8")).lower()
        if encoding in {"utf-8", "utf8", "utf-8-sig"}:
            shutil.copyfile(src, dest)  # DuckDB handles a UTF-8 BOM itself
        else:  # transcode to utf-8 so that bronze only ever sees one encoding
            try:
                with src.open("r", encoding=encoding, newline="") as fin, dest.open(
                    "w", encoding="utf-8", newline=""
                ) as fout:
                    shutil.copyfileobj(fin, fout)
            except (UnicodeDecodeError, LookupError) as exc:
                raise ExtractError(f"[{self.source.name}] cannot decode {src} as {encoding}: {exc}") from exc
        return ExtractResult(path=dest, fingerprint=fp)
