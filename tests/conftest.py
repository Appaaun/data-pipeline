from __future__ import annotations

import csv
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from pipeline.config import PipelineConfig, config_from_dict

REPO = Path(__file__).resolve().parents[1]


def col(name: str, type: str = "VARCHAR", **kw: Any) -> dict[str, Any]:
    return {"name": name, "type": type, **kw}


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return path


def csv_source(**over: Any) -> dict[str, Any]:
    """A small csv source: items(id, name, qty, kind)."""
    src: dict[str, Any] = {
        "name": "items",
        "type": "csv",
        "options": {"path": "in/items.csv"},
        "primary_key": ["id"],
        "columns": [
            col("id", "INTEGER", nullable=False),
            col("name", nullable=False),
            col("qty", "INTEGER"),
            col("kind"),
        ],
        "quality": [
            {"rule": "range", "column": "qty", "min": 0},
            {"rule": "accepted_values", "column": "kind", "values": ["a", "b"]},
        ],
    }
    src.update(over)
    return src


@pytest.fixture
def make_cfg(tmp_path: Path):
    def _make(*sources: dict[str, Any], gold: dict[str, str] | None = None, **project: Any) -> PipelineConfig:
        raw = {"project": {"landing_retention_days": 7, **project}, "sources": list(sources)}
        for name, sql in (gold or {}).items():
            (tmp_path / "sql" / "gold").mkdir(parents=True, exist_ok=True)
            (tmp_path / "sql" / "gold" / f"{name}.sql").write_text(sql, encoding="utf-8")
        return config_from_dict(raw, tmp_path)

    return _make


@pytest.fixture
def items_csv(tmp_path: Path):
    def _write(rows: list[list[Any]]) -> Path:
        return write_csv(tmp_path / "in" / "items.csv", ["id", "name", "qty", "kind"], rows)

    return _write


@pytest.fixture
def orders_db(tmp_path: Path):
    """sqlite db with an `orders` table; returns (db_path, helper to insert rows)."""
    db = tmp_path / "shop.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, qty INTEGER, status TEXT, updated_at TEXT)")
    con.commit()

    def insert(*rows: tuple) -> None:
        c = sqlite3.connect(db)
        c.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?)", rows)
        c.commit()
        c.close()

    con.close()
    return db, insert


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Copy the real config + gold SQL + sample csv into tmp_path (the end-to-end fixture)."""
    shutil.copytree(REPO / "config", tmp_path / "config")
    shutil.copytree(REPO / "sql", tmp_path / "sql")
    (tmp_path / "data" / "sample").mkdir(parents=True)
    shutil.copy(REPO / "data" / "sample" / "pharmacy_stock.csv", tmp_path / "data" / "sample" / "pharmacy_stock.csv")
    return tmp_path
