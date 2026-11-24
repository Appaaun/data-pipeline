"""Generate demo inputs: data/sample/shop.db (orders) and .env. Safe to run repeatedly.

  pipeline init-demo                      create shop.db with ~3000 historical orders (if missing)
  pipeline init-demo --append-orders 200  simulate a day of new data: 200 new orders and a handful of
                                          status updates on old ones (shows incremental load + upsert)
"""

from __future__ import annotations

import csv
import datetime as dt
import random
import sqlite3
from pathlib import Path

CHANNELS = ["pos", "online", "line_oa"]
STATUSES = ["completed", "completed", "completed", "shipped", "pending", "cancelled"]


def _base_id(product_id: str) -> str:
    """PROD-1001-B2 (batch row) -> PROD-1001 (what customers actually buy)."""
    return product_id.split("-B")[0]


def _product_ids(root: Path) -> list[str]:
    csv_path = root / "data" / "sample" / "pharmacy_stock.csv"
    if csv_path.exists():
        with csv_path.open(encoding="utf-8-sig", newline="") as fh:
            ids = sorted({_base_id(r["ProductID"]) for r in csv.DictReader(fh) if r.get("ProductID")})
        if ids:
            return ids
    return [f"PROD-{1001 + i}" for i in range(78)]


def _price_lookup(root: Path) -> dict[str, float]:
    csv_path = root / "data" / "sample" / "pharmacy_stock.csv"
    prices: dict[str, float] = {}
    if csv_path.exists():
        with csv_path.open(encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    prices.setdefault(_base_id(r["ProductID"]), float(r["Unit_Price_THB"]))
                except (KeyError, ValueError):
                    pass
    return prices


def _ts(d: dt.datetime) -> str:
    return d.replace(microsecond=0).isoformat(sep=" ")


def generate_demo(root: Path, new_orders: int = 0, seed: int = 42) -> list[str]:
    log: list[str] = []
    sample = root / "data" / "sample"
    sample.mkdir(parents=True, exist_ok=True)
    db_path = sample / "shop.db"
    rng = random.Random(seed + new_orders)
    products = _product_ids(root)
    prices = _price_lookup(root)

    env = root / ".env"
    if not env.exists():
        env.write_text("DEMO_API_TOKEN=demo-token\n", encoding="utf-8")
        log.append(f"created {env.name} (demo API token)")

    con = sqlite3.connect(db_path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY, product_id TEXT, customer_id TEXT, quantity INTEGER,
            unit_price REAL, status TEXT, channel TEXT, ordered_at TEXT, updated_at TEXT)"""
    )
    (count,) = con.execute("SELECT count(*) FROM orders").fetchone()
    now = dt.datetime.now()

    def new_row(order_id: int, ordered: dt.datetime) -> tuple:
        pid = rng.choice(products)
        price = prices.get(pid, round(rng.uniform(40, 900), 2))
        status = rng.choice(STATUSES)
        return (order_id, pid, f"C{rng.randint(1, 800):04d}", rng.randint(1, 8), price, status,
                rng.choice(CHANNELS), _ts(ordered), _ts(ordered))

    if count == 0:
        rows = [new_row(i, now - dt.timedelta(minutes=rng.randint(60 * 24, 60 * 24 * 90))) for i in range(1, 3001)]
        for i in (40, 41, 900):  # dirty rows for the data-quality demo
            rows[i] = rows[i][:3] + (-3,) + rows[i][4:]  # negative quantity
        rows[1500] = rows[1500][:1] + (None,) + rows[1500][2:]  # missing product
        rows[2000] = rows[2000][:5] + ("teleported",) + rows[2000][6:]  # unknown status
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)", rows)
        log.append(f"created {db_path.relative_to(root)} with {len(rows)} orders (5 deliberately dirty)")
    if new_orders:
        (max_id,) = con.execute("SELECT max(order_id) FROM orders").fetchone()
        rows = [new_row(max_id + 1 + i, now - dt.timedelta(minutes=rng.randint(0, 60 * 6))) for i in range(new_orders)]
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)", rows)
        pending = con.execute("SELECT order_id FROM orders WHERE status IN ('pending','shipped') ORDER BY random() LIMIT 25").fetchall()
        for (oid,) in pending:
            con.execute("UPDATE orders SET status = 'completed', updated_at = ? WHERE order_id = ?", (_ts(now), oid))
        log.append(f"appended {len(rows)} new orders and updated {len(pending)} existing ones")
    elif count:
        log.append(f"{db_path.relative_to(root)} already has {count} orders (use --append-orders N to simulate new data)")
    con.commit()
    con.close()
    return log
