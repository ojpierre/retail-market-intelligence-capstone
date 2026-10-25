-- Kenya National Retail and Market Intelligence Platform
-- Core Analytical Business & Inflation Queries

-- 1. Price Competitiveness: Supermarket Shelf Price vs. KAMIS County Market Benchmark
SELECT 
    st.county,
    pr.product_name,
    pr.kamis_commodity_match AS market_commodity,
    ROUND(AVG(pi.retail_shelf_price_kes), 2) AS avg_shelf_price_kes,
    ROUND(AVG(pi.kamis_retail_benchmark_kes), 2) AS avg_market_benchmark_kes,
    ROUND(AVG(pi.spread_vs_market_kes), 2) AS avg_spread_kes,
    ROUND(AVG(pi.competitiveness_index_pct), 2) AS competitiveness_index_pct
FROM fact_price_intelligence pi
JOIN dim_store st ON pi.store_key = st.store_key
JOIN dim_product pr ON pi.product_key = pr.product_key
JOIN dim_date dt ON pi.date_key = dt.date_key
WHERE dt.year = 2024
GROUP BY st.county, pr.product_name, pr.kamis_commodity_match
ORDER BY competitiveness_index_pct DESC
LIMIT 15;

-- 2. Real vs. Nominal Sales Growth Deflated by KNBS Food CPI Index
SELECT 
    dt.year,
    dt.month,
    dt.month_name,
    ROUND(AVG(cpi.cpi_food), 2) AS knbs_food_cpi,
    ROUND(AVG(cpi.food_inflation_yoy), 2) AS knbs_food_inflation_yoy,
    ROUND(SUM(s.nominal_revenue_kes), 2) AS total_nominal_revenue_kes,
    ROUND(SUM(s.real_revenue_kes), 2) AS total_real_revenue_deflated_kes,
    ROUND((SUM(s.nominal_revenue_kes) - SUM(s.real_revenue_kes)), 2) AS inflation_distortion_kes
FROM fact_pos_sales s
JOIN dim_date dt ON s.date_key = dt.date_key
JOIN dim_knbs_cpi cpi ON dt.year = cpi.year AND dt.month = cpi.month
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- 3. Store Performance & Profitability Rankings Across Counties
SELECT 
    st.store_id,
    st.store_name,
    st.county,
    st.region,
    st.store_format,
    COUNT(s.sales_id) AS total_transactions,
    ROUND(SUM(s.nominal_revenue_kes), 2) AS gross_revenue_kes,
    ROUND(SUM(s.gross_profit_kes), 2) AS gross_profit_kes,
    ROUND((SUM(s.gross_profit_kes) / SUM(s.nominal_revenue_kes)) * 100.0, 2) AS gross_margin_pct
FROM fact_pos_sales s
JOIN dim_store st ON s.store_key = st.store_key
GROUP BY st.store_id, st.store_name, st.county, st.region, st.store_format
ORDER BY gross_revenue_kes DESC;

-- 4. Supplier Performance, Value Delivery, and Profit Margins
SELECT 
    sup.supplier_id,
    sup.supplier_name,
    sup.category_focus,
    sup.lead_time_days,
    sup.rating,
    COUNT(s.sales_id) AS items_sold_count,
    ROUND(SUM(s.nominal_revenue_kes), 2) AS total_sales_generated_kes,
    ROUND(SUM(s.gross_profit_kes), 2) AS retail_gross_profit_kes,
    ROUND((SUM(s.gross_profit_kes) / SUM(s.nominal_revenue_kes)) * 100.0, 2) AS supplier_profit_margin_pct
FROM fact_pos_sales s
JOIN dim_supplier sup ON s.supplier_key = sup.supplier_key
GROUP BY sup.supplier_id, sup.supplier_name, sup.category_focus, sup.lead_time_days, sup.rating
ORDER BY total_sales_generated_kes DESC;

-- 5. Inventory Stockout Risk and Critical Supply Shortages by Store
SELECT 
    st.store_name,
    st.county,
    pr.product_name,
    pr.category,
    inv.stock_on_hand AS current_stock,
    inv.reorder_threshold,
    COUNT(CASE WHEN inv.is_below_reorder = TRUE THEN 1 END) AS stockout_alert_events
FROM fact_inventory_snapshot inv
JOIN dim_store st ON inv.store_key = st.store_key
JOIN dim_product pr ON inv.product_key = pr.product_key
GROUP BY st.store_name, st.county, pr.product_name, pr.category, inv.stock_on_hand, inv.reorder_threshold
HAVING inv.stock_on_hand < inv.reorder_threshold
ORDER BY inv.stock_on_hand ASC
LIMIT 15;
