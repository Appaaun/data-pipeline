-- description: Units sold per product over the last 7 / 30 days (basis for reorder planning)
SELECT
    product_id,
    sum(quantity) FILTER (WHERE ordered_at >= now()::TIMESTAMP - INTERVAL 7 DAY)  AS units_7d,
    sum(quantity) FILTER (WHERE ordered_at >= now()::TIMESTAMP - INTERVAL 30 DAY) AS units_30d
FROM silver.shop_orders
WHERE status <> 'cancelled'
GROUP BY product_id
