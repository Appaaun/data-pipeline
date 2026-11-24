"""REST extractor against the in-process mock API, plus gold-model DAG behaviour."""

import duckdb
import pytest

from pipeline.demo.mock_api import make_movements, start_background
from pipeline.errors import ConfigError
from pipeline.layers.gold import discover_models
from pipeline.runner import Runner


@pytest.fixture
def api():
    data = make_movements(250)
    server, url = start_background(data)
    yield url, data, server.RequestHandlerClass
    server.shutdown()


def api_source(url, token="demo-token", **over):
    src = {
        "name": "moves",
        "type": "api",
        "options": {
            "url": f"{url}/api/stock_movements",
            "auth": {"type": "bearer", "token": token},
            "records_path": "data.items",
            "pagination": {"type": "page", "page_size": 100},
            "retries": 0,
        },
        "load_mode": "incremental",
        "incremental": {"param": "since", "column": "updated_at"},
        "primary_key": ["movement_id"],
        "columns": [
            {"name": "movement_id", "type": "BIGINT", "nullable": False},
            {"name": "movement_type", "type": "VARCHAR"},
            {"source": "warehouse_code", "name": "warehouse", "type": "VARCHAR"},
            {"name": "updated_at", "type": "TIMESTAMP", "nullable": False},
        ],
        "quality": [{"rule": "accepted_values", "column": "movement_type", "values": ["IN", "OUT", "ADJUST"]},
                    {"rule": "max_quarantine_ratio", "value": 0.1}],
    }
    src.update(over)
    return src


def test_paginates_flattens_and_goes_incremental(make_cfg, api):
    url, data, handler = api
    cfg = make_cfg(api_source(url))
    with Runner(cfg) as r:
        s1 = r.run(skip_gold=True)
        assert s1.ok and s1.sources[0].rows_in == 250
        assert len([p for p in handler.requests_seen if "page=" in p]) == 3  # 100 + 100 + 50
        p = (cfg.silver_dir / "moves.parquet").as_posix()
        assert duckdb.sql(f"SELECT count(DISTINCT warehouse) FROM read_parquet('{p}')").fetchall()[0][0] == 3  # nested -> flat
        handler.requests_seen.clear()
        s2 = r.run(skip_gold=True)  # watermark -> server returns nothing new
        assert s2.sources[0].status == "skipped"
        assert any("since=" in p for p in handler.requests_seen)


def test_bad_token_fails_only_that_source(make_cfg, api):
    url, *_ = api
    cfg = make_cfg(api_source(url, token="wrong"))
    with Runner(cfg) as r:
        s = r.run(skip_gold=True)
    assert s.sources[0].status == "failed" and "401" in s.sources[0].message


def test_unknown_records_path_gives_clear_error(make_cfg, api):
    url, *_ = api
    src = api_source(url)
    src["options"]["records_path"] = "data.nothing"
    with Runner(make_cfg(src)) as r:
        s = r.run(skip_gold=True)
    assert "records_path" in s.sources[0].message


# ----------------------------------------------------------------------------- gold


def test_gold_models_sorted_by_dependency(tmp_path):
    d = tmp_path / "g"
    d.mkdir()
    (d / "a.sql").write_text("-- depends: b\nSELECT 1 AS x")
    (d / "b.sql").write_text("-- depends: c\nSELECT 1 AS x")
    (d / "c.sql").write_text("SELECT 1 AS x")
    assert [m.name for m in discover_models(d)] == ["c", "b", "a"]


def test_gold_cycle_and_unknown_dependency_detected(tmp_path):
    d = tmp_path / "g"
    d.mkdir()
    (d / "a.sql").write_text("-- depends: b\nSELECT 1")
    (d / "b.sql").write_text("-- depends: a\nSELECT 1")
    with pytest.raises(ConfigError, match="cycle"):
        discover_models(d)
    (d / "b.sql").write_text("-- depends: ghost\nSELECT 1")
    with pytest.raises(ConfigError, match="unknown model"):
        discover_models(d)


def test_failed_gold_model_skips_dependents_but_not_siblings(make_cfg, items_csv):
    from conftest import csv_source

    cfg = make_cfg(
        csv_source(),
        gold={
            "broken": "SELECT * FROM silver.does_not_exist",
            "child": "-- depends: broken\nSELECT * FROM gold.broken",
            "fine": "SELECT count(*) AS n FROM silver.items",
        },
    )
    items_csv([[1, "a", 1, "a"]])
    with Runner(cfg) as r:
        s = r.run()
        status = dict(r.wh.execute("SELECT source, status FROM meta.stage_runs WHERE stage = 'gold'").fetchall())
    assert s.gold == "failed" and not s.ok
    assert status == {"broken": "failed", "child": "skipped", "fine": "success"}
    assert (cfg.gold_dir / "fine.parquet").exists()


def test_gold_skipped_when_a_source_failed(make_cfg, tmp_path):
    from conftest import csv_source

    cfg = make_cfg(csv_source(), gold={"m": "SELECT 1 AS x"})  # in/items.csv does not exist -> extract fails
    with Runner(cfg) as r:
        s = r.run()
    assert s.sources[0].status == "failed" and s.gold == "skipped"
