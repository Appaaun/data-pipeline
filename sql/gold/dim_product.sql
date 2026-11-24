-- description: One row per product (batch rows PROD-nnnn-Bn rolled up to PROD-nnnn)
SELECT
    regexp_extract(product_id, '^(PROD-[0-9]+)', 1)  AS product_id,
    any_value(product_name)                          AS product_name,
    any_value(category)                              AS category,
    any_value(supplier)                              AS supplier,
    count(*)                                         AS batch_count,
    sum(current_stock)                               AS current_stock,
    max(reorder_point)                               AS reorder_point,
    max(unit_cost_thb)                               AS unit_cost_thb,
    max(unit_price_thb)                              AS unit_price_thb,
    min(expiry_date)                                 AS earliest_expiry_date
FROM silver.pharmacy_stock
GROUP BY 1
