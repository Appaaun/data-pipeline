-- description: Stock value and risk indicators per product category
SELECT
    category,
    count(*)                                                        AS sku_count,
    sum(current_stock)                                              AS units_on_hand,
    round(sum(current_stock * unit_cost_thb), 2)                    AS stock_value_at_cost_thb,
    round(sum(current_stock * unit_price_thb), 2)                   AS stock_value_at_price_thb,
    count(*) FILTER (WHERE stock_status = 'Critical Low')           AS critical_low_skus,
    count(*) FILTER (WHERE stock_status = 'Reorder Needed')         AS reorder_needed_skus,
    count(*) FILTER (WHERE expiry_date < current_date)              AS expired_skus,
    round(sum(current_stock * unit_cost_thb) FILTER (WHERE expiry_date < current_date), 2)
                                                                    AS expired_value_at_cost_thb,
    count(*) FILTER (WHERE expiry_date BETWEEN current_date AND current_date + INTERVAL 90 DAY)
                                                                    AS expiring_within_90d_skus
FROM silver.pharmacy_stock
GROUP BY category
ORDER BY stock_value_at_cost_thb DESC
