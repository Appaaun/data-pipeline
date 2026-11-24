"""REST/JSON API source with retries, auth, pagination and incremental `since` parameter."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..errors import ConfigError, ExtractError
from .base import ExtractResult, Extractor, csv_cell, write_csv

log = logging.getLogger("pipeline.extract.api")


def dig(obj: Any, path: str | None) -> Any:
    """Follow a dotted path ('data.items') into nested dicts. Empty path returns obj."""
    if not path:
        return obj
    for part in path.split("."):
        if isinstance(obj, dict) and part in obj:
            obj = obj[part]
        else:
            return None
    return obj


def flatten(record: dict[str, Any], sep: str = "_") -> dict[str, Any]:
    """Flatten nested dicts (a.b -> a_b). Lists are kept as JSON strings."""
    out: dict[str, Any] = {}

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                walk(f"{prefix}{sep}{k}" if prefix else str(k), v)
        elif isinstance(value, list):
            out[prefix] = json.dumps(value, ensure_ascii=False)
        else:
            out[prefix] = value

    walk("", record)
    return out


class ApiExtractor(Extractor):
    """options:
        url, method (GET), params {}, headers {}, timeout (30), retries (5), request_delay_seconds (0)
        auth:        {type: bearer, token: ${TOKEN}}  |  {type: header, name: X-Api-Key, value: ${KEY}}
        records_path: dotted path to the list of records in the response (default: response itself)
        pagination:  {type: none}
                     {type: page, page_param: page, size_param: page_size, page_size: 100, start_page: 1}
                     {type: cursor, cursor_param: cursor, next_cursor_path: meta.next, page_size_param, page_size}
                     max_pages: safety stop (default 10000)
        fields:      optional fixed column list (otherwise union of keys, first-seen order)
        flatten:     flatten nested objects (default true)
    incremental:
        param:         query parameter that receives the last watermark (e.g. since)
        column:        record field used to compute the new watermark (e.g. updated_at)
        initial_value: value for the first run
    """

    def _session(self) -> requests.Session:
        s = requests.Session()
        retry = Retry(
            total=int(self.options.get("retries", 5)),
            backoff_factor=float(self.options.get("backoff_factor", 1.0)),
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        s.mount("http://", HTTPAdapter(max_retries=retry))
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.headers.update(self.options.get("headers") or {})
        auth = self.options.get("auth") or {}
        if auth.get("type") == "bearer":
            s.headers["Authorization"] = f"Bearer {auth['token']}"
        elif auth.get("type") == "header":
            s.headers[auth["name"]] = auth["value"]
        elif auth:
            raise ConfigError(f"[{self.source.name}] unsupported auth type {auth.get('type')!r}")
        return s

    def _fetch_pages(self, session: requests.Session, params: dict[str, Any]):
        url = self.options.get("url")
        if not url:
            raise ExtractError(f"[{self.source.name}] api source needs options.url")
        method = str(self.options.get("method", "GET")).upper()
        timeout = float(self.options.get("timeout", 30))
        delay = float(self.options.get("request_delay_seconds", 0))
        pg = self.options.get("pagination") or {"type": "none"}
        ptype = pg.get("type", "none")
        max_pages = int(pg.get("max_pages", 10_000))
        records_path = self.options.get("records_path")

        page = int(pg.get("start_page", 1))
        cursor: str | None = None
        for n in range(1, max_pages + 1):
            q = dict(params)
            if ptype == "page":
                q[pg.get("page_param", "page")] = page
                q[pg.get("size_param", "page_size")] = int(pg.get("page_size", 100))
            elif ptype == "cursor":
                if cursor:
                    q[pg.get("cursor_param", "cursor")] = cursor
                if pg.get("page_size_param"):
                    q[pg["page_size_param"]] = int(pg.get("page_size", 100))
            elif ptype != "none":
                raise ConfigError(f"[{self.source.name}] unknown pagination type {ptype!r}")

            try:
                resp = session.request(method, url, params=q, timeout=timeout)
                resp.raise_for_status()
                body = resp.json()
            except requests.RequestException as exc:
                raise ExtractError(f"[{self.source.name}] request failed (page {n}): {exc}") from exc
            except ValueError as exc:
                raise ExtractError(f"[{self.source.name}] response is not valid JSON (page {n})") from exc

            records = dig(body, records_path)
            if records is None:
                raise ExtractError(
                    f"[{self.source.name}] records_path {records_path!r} not found in response (page {n})"
                )
            if not isinstance(records, list):
                raise ExtractError(f"[{self.source.name}] records_path must point to a list (page {n})")
            log.info("[%s] page %d -> %d records", self.source.name, n, len(records))
            yield records

            if ptype == "none":
                return
            if ptype == "page":
                if len(records) < int(pg.get("page_size", 100)):
                    return
                page += 1
            else:  # cursor
                cursor = dig(body, pg.get("next_cursor_path"))
                if not cursor:
                    return
            if delay:
                time.sleep(delay)
        log.warning("[%s] stopped at max_pages=%d - result may be incomplete", self.source.name, max_pages)

    def extract(self, out_dir: Path, *, watermark: str | None, fingerprint: str | None) -> ExtractResult:
        inc = self.source.incremental
        params = dict(self.options.get("params") or {})
        if self.source.is_incremental:
            if not inc.get("param") or not inc.get("column"):
                raise ConfigError(f"[{self.source.name}] api incremental needs incremental.param and incremental.column")
            params[inc["param"]] = watermark if watermark is not None else inc.get("initial_value", "1970-01-01T00:00:00")

        do_flatten = bool(self.options.get("flatten", True))
        all_rows: list[dict[str, Any]] = []
        with self._session() as session:
            for batch in self._fetch_pages(session, params):
                for rec in batch:
                    if not isinstance(rec, dict):
                        raise ExtractError(f"[{self.source.name}] expected objects in records, got {type(rec).__name__}")
                    all_rows.append(flatten(rec) if do_flatten else rec)

        if not all_rows:
            return ExtractResult(skipped=True, reason="API returned no records")

        header: list[str] = list(self.options.get("fields") or [])
        if not header:
            seen: dict[str, None] = {}
            for r in all_rows:
                for k in r:
                    seen.setdefault(k, None)
            header = list(seen)

        new_wm = None
        if self.source.is_incremental:
            col = inc["column"]
            vals = [r[col] for r in all_rows if r.get(col) is not None]
            if vals:
                new_wm = csv_cell(max(vals))

        dest = self.target(out_dir)
        n = write_csv(
            dest,
            header,
            ([r.get(h) if not isinstance(r.get(h), (dict, list)) else json.dumps(r.get(h)) for h in header] for r in all_rows),
        )
        return ExtractResult(path=dest, rows=n, new_watermark=new_wm)
