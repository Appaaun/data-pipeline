# PIPE_LINE — Config-driven Data Pipeline (Bronze → Silver → Gold)

Batch data pipeline แบบ template ใช้ซ้ำได้กับทุก dataset: ดึงข้อมูลจาก **CSV / REST API / Database**
เก็บเป็น **Parquet** ผ่าน 3 ชั้น (medallion) ประมวลผลด้วย **DuckDB** พร้อม data quality, quarantine,
incremental load, run history และ test suite — รันบน Windows ได้ทันที ไม่ต้องใช้ Docker/server

```
 CSV ──┐                  ┌── landing ──┐   ┌─ bronze ─┐   ┌─ silver ─────────────┐   ┌─ gold ───────────┐
 API ──┼─▶ Extractor ───▶ │ raw CSV     │──▶│ all text │──▶│ typed + validated    │──▶│ SQL models       │
 DB  ──┘  (retry, auth,   │ (transient) │   │ + lineage│   │ dedup by primary key │   │ (DAG, depends:)  │
          pagination,     └─────────────┘   │ immutable│   │ bad rows→quarantine  │   │ tables + parquet │
          watermark)                        └──────────┘   └──────────────────────┘   └──────────────────┘
                                                 │                    │                        │
                                  meta.* : runs, stage metrics, watermarks, quality results (DuckDB)
```

## Quick start

```powershell
cd D:\data\PIPE_LINE
.\.venv\Scripts\activate
pip install -e ".[dev]"            # ครั้งแรกครั้งเดียว

pipeline init-demo                 # สร้าง sample sqlite shop.db + .env
pipeline validate                  # ตรวจ config + gold models
pipeline mock-api                  # เปิด terminal แยก: API จำลองสำหรับ source stock_movements
pipeline run                       # extract → bronze → silver → gold
pipeline status                    # ประวัติ run / metrics / watermark
pipeline quality                   # ผล data quality ล่าสุด
pipeline query "select * from gold.reorder_report limit 10"

pipeline init-demo --append-orders 200   # จำลองข้อมูลใหม่ในฐานข้อมูล
pipeline run                             # โหลดเฉพาะส่วนต่าง (incremental + upsert)
python -m pytest                         # 34 tests
```

## โครงสร้างโปรเจกต์

| Path | หน้าที่ |
|---|---|
| `config/pipeline.yaml` | **จุดเดียวที่ต้องแก้** เพื่อเพิ่ม source / schema / quality rules |
| `sql/gold/*.sql` | gold models (1 ไฟล์ = 1 ตาราง) ประกาศ dependency ด้วย `-- depends: a, b` |
| `src/pipeline/extractors/` | `csv_file`, `rest_api`, `database` (เพิ่ม source ชนิดใหม่ = 1 class) |
| `src/pipeline/layers/` | `bronze.py`, `silver.py` (typing + DQ + dedup + merge), `gold.py` (DAG) |
| `src/pipeline/runner.py` | orchestration, error isolation, state commit |
| `src/pipeline/warehouse.py` | DuckDB warehouse + ตาราง `meta.*` |
| `data/{landing,bronze,silver,gold,quarantine,warehouse}` | output ทั้งหมด (generated, อยู่ใน .gitignore) |
| `scripts/run_pipeline.ps1` | entry point สำหรับ Task Scheduler (exit code 0/1/2) |
| `tests/` | config, silver/DQ, incremental DB, API, gold DAG, end-to-end |

## แต่ละชั้นทำอะไร

| Layer | เนื้อหา | หลักการ |
|---|---|---|
| **landing** | ไฟล์ CSV ดิบที่ extractor เขียน (`landing/<source>/<run_id>/`) | ชั่วคราว ลบอัตโนมัติหลัง `landing_retention_days` |
| **bronze** | Parquet ต่อ run: ทุกคอลัมน์เป็น VARCHAR + `_run_id, _ingested_at, _row_number, _row_hash` | append-only ไม่แก้ไข → replay ได้เสมอ |
| **silver** | `silver/<source>.parquet` พิมพ์ type แล้ว, ผ่าน rules, 1 แถวต่อ primary key | publish แบบ atomic (เขียน tmp แล้ว rename) เฉพาะเมื่อผ่าน dataset checks |
| **quarantine** | แถวที่ผิด rule พร้อมเหตุผล (`_reasons`) และ record ดิบ (`_raw_record`) | ไม่ทิ้งข้อมูลเงียบ ๆ ตรวจ/แก้/replay ได้ |
| **gold** | ตารางพร้อมใช้งาน (BI / report) | SQL ล้วน, เรียงตาม dependency, 1 model พังไม่ล้ม model อื่น |

## เพิ่ม source ใหม่ (ทั้งหมดอยู่ใน YAML)

```yaml
- name: my_source
  type: database                  # csv | api | database
  options: {url: "${SHOP_DB_URL}", table: orders}   # postgresql+psycopg://user:pass@host/db
  load_mode: incremental          # full | incremental
  incremental: {column: updated_at, initial_value: "1970-01-01 00:00:00"}
  primary_key: [order_id]
  columns:
    - {name: order_id, type: BIGINT, nullable: false}
    - {source: Amt, name: amount, type: "DECIMAL(12,2)"}   # source = ชื่อเดิม, name = ชื่อใน silver
  quality:
    - {rule: range, column: amount, min: 0}
```

Secrets ใส่ใน `.env` แล้วอ้างด้วย `${VAR}` / `${VAR:-default}` (ไม่ commit ค่า credential)

### Data quality rules

| Rule | ระดับ | พฤติกรรม |
|---|---|---|
| `not_null`, `range(min,max)`, `accepted_values(values)`, `regex(pattern)`, `expression(expr)` | row | `severity: error` → แถวเข้า quarantine · `severity: warn` → เก็บแถวไว้ แต่นับ |
| `min_rows`, `max_quarantine_ratio`, `max_row_drop_pct`, `freshness(column,max_age_days)` | dataset | `error` → **ไม่ publish silver** และ run ของ source นั้น failed · `warn` → log อย่างเดียว |

อัตโนมัติ: คอลัมน์ `nullable: false` และ primary key มี `not_null` เสมอ, type cast ที่ล้มเหลวถูกนับเป็น violation (`type_cast:<col>`) ไม่กลายเป็น NULL เงียบ ๆ

## ความถูกต้องของการ run (สำคัญ)

- **Idempotent**: รันซ้ำได้ ไม่ซ้ำข้อมูล (dedup ด้วย primary key, CSV ที่ไม่เปลี่ยน → `skipped` ด้วย SHA-256)
- **Watermark / fingerprint commit หลัง silver สำเร็จเท่านั้น**: run ที่พังจะ retry ช่วงเดิมเองในรอบหน้า (at-least-once + dedup = effectively-once)
- **Source isolation**: source หนึ่งพัง source อื่นยังรันต่อ; gold ถูกข้ามถ้ามี source ล้มเหลว; exit code ≠ 0 เพื่อให้ scheduler แจ้งเตือน
- **Schema contract**: คอลัมน์ที่ประกาศไว้หายไป → `SchemaError`; คอลัมน์ใหม่ที่ไม่ได้ประกาศ → warning (schema drift) ไม่พัง
- **Replay**: แก้ rule/type แล้ว `pipeline reprocess <source>` สร้าง silver ใหม่จาก bronze โดยไม่ต้องยิง source ซ้ำ
- `pipeline run --full-refresh` ล้าง watermark แล้วแทนที่ silver ทั้งหมด

## Observability

ทุกอย่างอยู่ใน `data/warehouse/warehouse.duckdb` (เปิดด้วย DBeaver/duckdb CLI ได้ แต่ปิดก่อนรัน pipeline — DuckDB เปิดได้ทีละ process):

```sql
select * from meta.pipeline_runs order by started_at desc;           -- ประวัติ run
select * from meta.stage_runs where run_id = '...';                  -- rows_in/out/quarantined ต่อ stage
select * from meta.quality_results where outcome <> 'pass';          -- rule ที่มี violation
select * from gold.pipeline_health;                                  -- สรุปล่าสุดต่อ source (quarantine %)
```

Log: console + `logs/pipeline.log` (rotating) ทุกบรรทัดมี `run_id`

## ตั้งเวลารัน (Windows Task Scheduler)

```powershell
schtasks /Create /TN "PIPE_LINE daily" /SC DAILY /ST 02:00 /TR "powershell -NoProfile -ExecutionPolicy Bypass -File D:\data\PIPE_LINE\scripts\run_pipeline.ps1"
```

## ข้อจำกัดที่ควรรู้ / ขั้นต่อไป

- Single-writer: DuckDB เปิดไฟล์ได้ทีละ process → ห้ามรัน 2 run พร้อมกัน
- API extractor เก็บ record ทั้งหมดในหน่วยความจำก่อนเขียน (เหมาะกับหลักแสนแถว); DB extractor stream เป็น chunk
- Watermark เทียบแบบ `>` — ถ้า source มี late-arriving data ให้ถอย watermark (lookback) หรือใช้ `--full-refresh` เป็นระยะ
- Incremental upsert เลือกแถวที่ "มาทีหลัง" ชนะ ไม่ได้เทียบ `updated_at` (ถ้ากังวลเรื่อง replay ย้อนหลังให้ใช้ `reprocess`)
- ไม่มี delete detection สำหรับ incremental (ต้องมี soft-delete flag ที่ source)
- ต่อยอดได้: alert (Line Notify/Slack) เมื่อ run ล้มเหลว, Dagster/Airflow ครอบ `Runner.run()`, dbt แทน gold SQL, เปลี่ยน storage เป็น S3/MinIO, เพิ่ม extractor (SFTP, Excel, Google Sheets)

## ข้อมูลตัวอย่างที่ใช้

- `pharmacy_stock` ← `data/sample/pharmacy_stock.csv` (สำเนาจาก `D:\data\pharmacy_and_inventory_stock_large.csv`): 156 แถว = 78 สินค้า × 2 batch (`PROD-1001`, `PROD-1001-B2`)
- `shop_orders` ← sqlite `data/sample/shop.db` ที่ `init-demo` สร้าง (3,000 orders, มีแถวเสียตั้งใจ 5 แถว)
- `stock_movements` ← mock REST API (`pipeline mock-api`, มีแถวเสียตั้งใจ 3 แถว)
