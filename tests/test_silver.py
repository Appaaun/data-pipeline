"""Silver layer: typing, row-level rules, quarantine, de-dup, dataset rules, idempotency."""

import duckdb

from pipeline.runner import Runner

from conftest import csv_source


def run(cfg, **kw):
    with Runner(cfg) as r:
        return r.run(**kw)


def silver(cfg, name="items"):
    return duckdb.sql(f"SELECT * FROM read_parquet('{(cfg.silver_dir / (name + '.parquet')).as_posix()}') ORDER BY 1").fetchall()


def quarantine(cfg, name="items"):
    files = list((cfg.quarantine_dir / name).glob("*.parquet"))
    assert len(files) == 1
    return duckdb.sql(f"SELECT _reasons FROM read_parquet('{files[0].as_posix()}')").fetchall()


def test_clean_load(make_cfg, items_csv):
    cfg = make_cfg(csv_source(), gold={})
    items_csv([[1, " apple ", 5, "a"], [2, "pear", 0, "b"]])
    s = run(cfg, skip_gold=True)
    assert s.ok
    rows = silver(cfg)
    assert [r[:4] for r in rows] == [(1, "apple", 5, "a"), (2, "pear", 0, "b")]  # trimmed + typed


def test_bad_rows_are_quarantined_with_reasons(make_cfg, items_csv):
    cfg = make_cfg(csv_source(quality=csv_source()["quality"] + [{"rule": "max_quarantine_ratio", "value": 0.9}]))
    items_csv([
        [1, "ok", 5, "a"],
        [2, "neg", -1, "a"],       # range
        [3, "badkind", 5, "zzz"],   # accepted_values
        [4, "notnum", "abc", "a"],  # type_cast
        [5, "", 5, "a"],            # not_null name
    ])
    s = run(cfg, skip_gold=True)
    assert s.ok
    assert [r[0] for r in silver(cfg)] == [1]
    reasons = sorted(r[0] for r in quarantine(cfg))
    assert reasons == ["accepted_values:kind", "not_null:name", "range:qty", "type_cast:qty"]


def test_warn_rules_keep_the_row(make_cfg, items_csv):
    q = [{"rule": "range", "column": "qty", "max": 10, "severity": "warn"}]
    cfg = make_cfg(csv_source(quality=q))
    items_csv([[1, "a", 5, "a"], [2, "b", 99, "a"]])
    run(cfg, skip_gold=True)
    assert len(silver(cfg)) == 2
    with Runner(cfg) as r:
        row = r.wh.execute("SELECT violations, outcome FROM meta.quality_results WHERE rule = 'range:qty'").fetchone()
    assert row == (1, "warn")


def test_duplicate_primary_keys_keep_last(make_cfg, items_csv):
    cfg = make_cfg(csv_source())
    items_csv([[1, "first", 1, "a"], [2, "x", 1, "a"], [1, "second", 2, "a"]])
    run(cfg, skip_gold=True)
    rows = silver(cfg)
    assert len(rows) == 2 and rows[0][1] == "second"


def test_quarantine_ratio_blocks_publish(make_cfg, items_csv):
    q = csv_source()["quality"] + [{"rule": "max_quarantine_ratio", "value": 0.2}]
    cfg = make_cfg(csv_source(quality=q))
    items_csv([[1, "ok", 1, "a"], [2, "bad", -5, "a"], [3, "bad", -5, "a"]])
    s = run(cfg, skip_gold=True)
    assert not s.ok
    assert "max_quarantine_ratio" in s.sources[0].message
    assert not (cfg.silver_dir / "items.parquet").exists()  # nothing published
    assert (cfg.bronze_dir / "items").exists()              # but raw data is kept for replay


def test_failed_run_does_not_commit_fingerprint_so_retry_reprocesses(make_cfg, items_csv):
    q = csv_source()["quality"] + [{"rule": "max_quarantine_ratio", "value": 0.2}]
    cfg = make_cfg(csv_source(quality=q))
    items_csv([[1, "ok", 1, "a"], [2, "bad", -5, "a"]])
    assert not run(cfg, skip_gold=True).ok
    assert not run(cfg, skip_gold=True).ok  # same file again -> must NOT be skipped as "unchanged"


def test_unchanged_csv_is_skipped(make_cfg, items_csv):
    cfg = make_cfg(csv_source())
    items_csv([[1, "a", 1, "a"]])
    assert run(cfg, skip_gold=True).sources[0].status == "success"
    assert run(cfg, skip_gold=True).sources[0].status == "skipped"
    assert run(cfg, skip_gold=True, full_refresh=True).sources[0].status == "success"


def test_missing_declared_column_fails_with_schema_error(make_cfg, tmp_path):
    from conftest import write_csv

    cfg = make_cfg(csv_source())
    write_csv(tmp_path / "in" / "items.csv", ["id", "name", "qty"], [[1, "a", 1]])  # 'kind' vanished
    s = run(cfg, skip_gold=True)
    assert s.sources[0].status == "failed" and "kind" in s.sources[0].message


def test_undeclared_extra_column_is_ignored(make_cfg, tmp_path):
    from conftest import write_csv

    cfg = make_cfg(csv_source())
    write_csv(tmp_path / "in" / "items.csv", ["id", "name", "qty", "kind", "surprise"], [[1, "a", 1, "a", "x"]])
    assert run(cfg, skip_gold=True).ok


def test_reprocess_applies_new_rules_to_existing_bronze(make_cfg, items_csv):
    cfg = make_cfg(csv_source())
    items_csv([[1, "a", 5, "a"], [2, "b", 50, "a"]])
    run(cfg, skip_gold=True)
    assert len(silver(cfg)) == 2
    # tighten the rule, then rebuild silver from bronze without touching the source
    stricter = csv_source(quality=[{"rule": "range", "column": "qty", "max": 10}, {"rule": "max_quarantine_ratio", "value": 0.9}])
    cfg2 = make_cfg(stricter)
    with Runner(cfg2) as r:
        s = r.reprocess("items")
    assert s.ok
    assert [r[0] for r in silver(cfg2)] == [1]
