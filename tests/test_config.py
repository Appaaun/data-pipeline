import pytest

from pipeline.config import config_from_dict, expand_env
from pipeline.errors import ConfigError

from conftest import csv_source


def build(tmp_path, **over):
    return config_from_dict({"sources": [csv_source(**over)]}, tmp_path)


def test_valid_config_adds_implicit_not_null_rules(tmp_path):
    cfg = build(tmp_path)
    names = {r.name for r in cfg.sources[0].quality}
    assert {"not_null:id", "not_null:name"} <= names  # nullable: false / primary key
    assert "not_null:qty" not in names


def test_unknown_rule_rejected(tmp_path):
    with pytest.raises(ConfigError, match="unknown rule"):
        build(tmp_path, quality=[{"rule": "magic", "column": "qty"}])


def test_rule_missing_param_rejected(tmp_path):
    with pytest.raises(ConfigError, match="requires 'values'"):
        build(tmp_path, quality=[{"rule": "accepted_values", "column": "kind"}])


def test_rule_unknown_column_rejected(tmp_path):
    with pytest.raises(ConfigError, match="unknown column"):
        build(tmp_path, quality=[{"rule": "not_null", "column": "nope"}])


def test_primary_key_must_exist(tmp_path):
    with pytest.raises(ConfigError, match="primary_key"):
        build(tmp_path, primary_key=["missing"])


def test_primary_key_required(tmp_path):
    with pytest.raises(ConfigError, match="needs a primary_key"):
        build(tmp_path, primary_key=[])


def test_csv_cannot_be_incremental(tmp_path):
    with pytest.raises(ConfigError, match="only support load_mode: full"):
        build(tmp_path, load_mode="incremental", incremental={"column": "id"})


def test_bad_column_type_rejected(tmp_path):
    cols = csv_source()["columns"] + [{"name": "x", "type": "VARCHAR); DROP TABLE t; --"}]
    with pytest.raises(ConfigError, match="unsupported type"):
        build(tmp_path, columns=cols)


def test_bad_identifier_rejected(tmp_path):
    cols = [{"name": "bad name", "type": "INTEGER"}]
    with pytest.raises(ConfigError):
        build(tmp_path, columns=cols, primary_key=["bad name"])


def test_duplicate_rule_names_are_disambiguated(tmp_path):
    q = [{"rule": "expression", "expr": "qty > 0", "name": "dup"}, {"rule": "expression", "expr": "qty < 9", "name": "dup"}]
    names = [r.name for r in build(tmp_path, quality=q).sources[0].quality]
    assert "dup" in names and "dup#2" in names


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("TOKEN_X", "s3cret")
    assert expand_env({"a": ["${TOKEN_X}", "${NOPE:-fallback}"]}) == {"a": ["s3cret", "fallback"]}
    with pytest.raises(ConfigError, match="NOPE_MISSING"):
        expand_env("${NOPE_MISSING}")
