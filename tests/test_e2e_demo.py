"""End to end: the shipped config + SQL + sample data, with all three source types."""

import duckdb
import pytest

from pipeline.cli import main
from pipeline.config import load_config
from pipeline.demo.generate import generate_demo
from pipeline.demo.mock_api import make_movements, start_background
from pipeline.runner import Runner


@pytest.fixture
def env(demo_project, monkeypatch):
    server, url = start_background(make_movements(300))
    monkeypatch.setenv("MOCK_API_URL", url)
    monkeypatch.setenv("DEMO_API_TOKEN", "demo-token")
    generate_demo(demo_project)
    yield demo_project
    server.shutdown()


def test_full_pipeline_then_incremental_rerun(env):
    cfg = load_config(env / "config" / "pipeline.yaml")
    with Runner(cfg) as r:
        s1 = r.run()
        assert s1.ok, [(o.source, o.message) for o in s1.sources]
        by = {o.source: o for o in s1.sources}
        assert by["pharmacy_stock"].rows_out == 156 and by["pharmacy_stock"].quarantined == 0
        assert by["shop_orders"].quarantined == 5      # the deliberately dirty rows
        assert by["stock_movements"].quarantined == 3
        assert s1.gold == "success"

        gold = {t for (t,) in r.wh.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='gold'").fetchall()}
        assert {"dim_product", "daily_sales", "inventory_health", "reorder_report", "sales_velocity", "pipeline_health"} <= gold
        # every order's product resolves in the dimension -> no 'Unknown' category in sales
        assert r.wh.execute("SELECT count(*) FROM gold.daily_sales WHERE category = 'Unknown'").fetchone()[0] == 0

        # second run: csv unchanged, no new rows anywhere -> everything skipped, gold rebuilt
        s2 = r.run()
        assert s2.ok and {o.status for o in s2.sources} == {"skipped"}

    # new data arrives in the source db -> only the delta is loaded, silver stays unique per key
    generate_demo(env, new_orders=50)
    with Runner(cfg) as r:
        s3 = r.run()
        assert s3.ok
        orders = {o.source: o for o in s3.sources}["shop_orders"]
        assert 50 <= orders.rows_in <= 75  # 50 new + up to 25 updated
        total, distinct = r.wh.execute("SELECT count(*), count(DISTINCT order_id) FROM silver.shop_orders").fetchone()
        assert total == distinct == 2995 + 50


def test_cli_validate_and_run_exit_codes(env, capsys):
    cfg_path = str(env / "config" / "pipeline.yaml")
    assert main(["-c", cfg_path, "validate"]) == 0
    assert main(["-c", cfg_path, "run", "--skip-gold"]) == 0
    assert main(["-c", cfg_path, "status"]) == 0
    assert main(["-c", cfg_path, "query", "select count(*) as n from silver.pharmacy_stock"]) == 0
    assert "156" in capsys.readouterr().out
    assert main(["-c", cfg_path, "run", "-s", "nope"]) == 2  # config error -> exit code 2
