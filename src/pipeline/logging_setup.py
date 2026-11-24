"""Logging: console + rotating file, every record tagged with the current run id."""

from __future__ import annotations

import contextvars
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")


class _RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_var.get()
        return True


def setup_logging(logs_dir: Path | None = None, level: str = "INFO") -> None:
    root = logging.getLogger("pipeline")
    root.setLevel(level.upper())
    root.handlers.clear()
    root.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(run_id)s %(name)s | %(message)s", "%H:%M:%S")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(_RunIdFilter())
    root.addHandler(console)

    if logs_dir is not None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            logs_dir / "pipeline.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
        )
        file_fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(run_id)s %(name)s | %(message)s")
        file_handler.setFormatter(file_fmt)
        file_handler.addFilter(_RunIdFilter())
        root.addHandler(file_handler)
