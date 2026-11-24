"""Database source: watermark-based incremental load + upsert semantics."""

import duckdb

from pipeline.runner import Runner


def orders_source(db, **over):
    src = {
        "name": "orders",
        "type": "database",
        "options": {"url": f"sqlite:///{db.as_posix()}", "table": "orders"},
        "load_mode": "incremental",
        "incremental": {"column": "updated_at", "initial_value": "1970-01-01 00:00:00"},
        "primary_key": ["id"],
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "qty", "type": "INTEGER"},
            {"name": "status", "type": "VARCHAR"},
            {"name": "updated_at", "type": "TIMESTAMP", "nullable": False},
        ],
        "quality": [{"rule": "range", "column": "qty", "min": 1}, {"rule": "max_quarantine_ratio", "value": 0.5}],
    }
    src.update(over)
    return src


def silver_rows(cfg):
    p = (cfg.silver_dir / "orders.parquet").as_posix()
    return duckdb.sql(f"SELECT id, qty, status FROM read_parquet('{p}') ORDER BY id").fetchall()


def run(cfg):
    with Runner(cfg) as r:
        return r.run(skip_gold=True)


def test_incremental_upsert_and_watermark(make_cfg, orders_db):
    db, insert = orders_db
    cfg = make_cfg(orders_source(db))
    insert((1, 5, "new", "2026-01-01 10:00:00"), (2, 3, "new", "2026-01-01 11:00:00"))

    s1 = run(cfg)
    assert s1.sources[0].status == "success" and s1.sources[0].rows_in == 2
    assert silver_rows(cfg) == [(1, 5, "new"), (2, 3, "new")]

    s2 = run(cfg)  # nothing changed
    assert s2.sources[0].status == "skipped"

    # order 2 is updated, order 3 is new -> only those two are pulled; silver is upserted
    insert((2, 3, "done", "2026-01-02 09:00:00"), (3, 7, "new", "2026-01-02 09:30:00"))
    s3 = run(cfg)
    assert s3.sources[0].rows_in == 2
    assert silver_rows(cfg) == [(1, 5, "new"), (2, 3, "done"), (3, 7, "new")]

    with Runner(cfg) as r:
        assert r.wh.get_watermark("orders") == "2026-01-02 09:30:00"


def test_watermark_not_advanced_when_silver_fails(make_cfg, orders_db):
    db, insert = orders_db
    cfg = make_cfg(orders_source(db, quality=[{"rule": "range", "column": "qty", "min": 1}, {"rule": "max_quarantine_ratio", "value": 0.1}]))
    insert((1, 5, "ok", "2026-01-01 10:00:00"), (2, -1, "bad", "2026-01-01 11:00:00"))
    s = run(cfg)
    assert s.sources[0].status == "failed"
    with Runner(cfg) as r:
        assert r.wh.get_watermark("orders") is None  # next run re-extracts the same window


def test_full_refresh_replaces_silver(make_cfg, orders_db):
    db, insert = orders_db
    cfg = make_cfg(orders_source(db))
    insert((1, 5, "a", "2026-01-01 10:00:00"), (2, 3, "b", "2026-01-01 11:00:00"))
    run(cfg)
    insert((2, -9, "b", "2026-01-03 00:00:00"), (3, 4, "c", "2026-01-03 00:01:00"))  # 2 becomes invalid
    run(cfg)
    assert silver_rows(cfg) == [(1, 5, "a"), (2, 3, "b"), (3, 4, "c")]  # old good version of 2 survives the upsert
    with Runner(cfg) as r:
        r.run(skip_gold=True, full_refresh=True)
    assert silver_rows(cfg) == [(1, 5, "a"), (3, 4, "c")]  # replace semantics: only currently valid rows


def test_custom_query_requires_watermark_placeholder(make_cfg, orders_db):
    db, _ = orders_db
    opts = {"url": f"sqlite:///{db.as_posix()}", "query": "SELECT * FROM orders"}
    cfg = make_cfg(orders_source(db, options=opts))
    s = run(cfg)
    assert s.sources[0].status == "failed" and ":watermark" in s.sources[0].message
