-- description: Stock rows at/below their reorder point, ranked by urgency, with sales-based days of cover
-- depends: sales_velocity
-- Works at batch level on purpose: reorder points are defined per stock row in the source system and
-- rolling batches up would hide a nearly-empty batch behind a full one.
WITH base AS (
    SELECT
        p.product_id                                      AS stock_row_id,
        regexp_extract(p.product_id, '^(PROD-[0-9]+)', 1) AS product_id,
        p.product_name, p.category, p.supplier,
        p.current_stock, p.reorder_point, p.unit_cost_thb, p.expiry_date, p.stock_status
    FROM silver.pharmacy_stock p
    WHERE p.current_stock <= p.reorder_point
),
calc AS (
    SELECT
        b.*,
        coalesce(v.units_30d, 0) AS product_units_30d,
        -- bring the row back to its reorder point plus one month of this product's sales
        greatest(0, CAST(coalesce(v.units_30d, 0) + b.reorder_point - b.current_stock AS INTEGER)) AS suggested_order_qty
    FROM base b
    LEFT JOIN gold.sales_velocity v USING (product_id)
)
SELECT
    *,
    CASE WHEN product_units_30d > 0 THEN round(current_stock / (product_units_30d / 30.0), 1) END AS days_of_cover,
    round(suggested_order_qty * unit_cost_thb, 2) AS suggested_order_cost_thb
FROM calc
ORDER BY current_stock * 1.0 / nullif(reorder_point, 0), current_stock
