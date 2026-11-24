-- description: Revenue and units per day and product category (cancelled orders excluded)
-- depends: dim_product
SELECT
    CAST(o.ordered_at AS DATE)               AS sales_date,
    coalesce(p.category, 'Unknown')          AS category,
    count(*)                                 AS orders,
    sum(o.quantity)                          AS units_sold,
    round(sum(o.quantity * o.unit_price), 2) AS revenue_thb
FROM silver.shop_orders o
LEFT JOIN gold.dim_product p USING (product_id)
WHERE o.status <> 'cancelled'
GROUP BY ALL
ORDER BY sales_date, category
