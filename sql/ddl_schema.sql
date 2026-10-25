-- Kenya National Retail and Market Intelligence Platform
-- Kimball Star Schema Warehouse DDL (PostgreSQL & SQLite compatible)

-- ============================================================================
-- 1. CONFORMED DIMENSION TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_harvest_season BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key INTEGER PRIMARY KEY,
    store_id VARCHAR(20) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    county VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    store_format VARCHAR(30) NOT NULL,
    sqm_size INTEGER NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    sku_code VARCHAR(30) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit_size VARCHAR(20) NOT NULL,
    standard_unit VARCHAR(10) NOT NULL,
    cost_price_kes DECIMAL(10, 2) NOT NULL,
    recommended_retail_price_kes DECIMAL(10, 2) NOT NULL,
    kamis_commodity_match VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key INTEGER PRIMARY KEY,
    supplier_id VARCHAR(20) NOT NULL UNIQUE,
    supplier_name VARCHAR(100) NOT NULL,
    category_focus VARCHAR(50) NOT NULL,
    lead_time_days INTEGER NOT NULL,
    rating DECIMAL(3, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_kamis_market (
    market_key INTEGER PRIMARY KEY,
    market_name VARCHAR(100) NOT NULL UNIQUE,
    county VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6)
);

CREATE TABLE IF NOT EXISTS dim_knbs_cpi (
    cpi_key INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    cpi_overall DECIMAL(6, 2) NOT NULL,
    cpi_food DECIMAL(6, 2) NOT NULL,
    cpi_housing_fuel DECIMAL(6, 2) NOT NULL,
    inflation_rate_yoy DECIMAL(5, 2) NOT NULL,
    food_inflation_yoy DECIMAL(5, 2) NOT NULL
);

-- ============================================================================
-- 2. FACT TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_pos_sales (
    sales_id INTEGER PRIMARY KEY,
    date_key INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    supplier_key INTEGER NOT NULL,
    transaction_id VARCHAR(50) NOT NULL,
    receipt_number VARCHAR(50) NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_kes DECIMAL(10, 2) NOT NULL,
    discount_kes DECIMAL(10, 2) NOT NULL DEFAULT 0.0,
    nominal_revenue_kes DECIMAL(10, 2) NOT NULL,
    real_revenue_kes DECIMAL(10, 2) NOT NULL,
    cost_price_kes DECIMAL(10, 2) NOT NULL,
    total_cost_kes DECIMAL(10, 2) NOT NULL,
    gross_profit_kes DECIMAL(10, 2) NOT NULL,
    gross_margin_pct DECIMAL(6, 2) NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    snapshot_id INTEGER PRIMARY KEY,
    date_key INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    movement_type VARCHAR(30) NOT NULL,
    quantity_delta INTEGER NOT NULL,
    stock_on_hand INTEGER NOT NULL,
    reorder_threshold INTEGER NOT NULL,
    is_below_reorder BOOLEAN NOT NULL DEFAULT FALSE,
    recorded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_kamis_market_price (
    price_id INTEGER PRIMARY KEY,
    date_key INTEGER NOT NULL,
    market_key INTEGER NOT NULL,
    commodity_name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    raw_unit VARCHAR(50) NOT NULL,
    std_unit VARCHAR(10) NOT NULL,
    wholesale_price_kes DECIMAL(10, 2) NOT NULL,
    norm_wholesale_price_kes DECIMAL(10, 2) NOT NULL,
    norm_retail_price_kes DECIMAL(10, 2) NOT NULL,
    wholesale_retail_spread_kes DECIMAL(10, 2),
    wholesale_markup_pct DECIMAL(6, 2)
);

CREATE TABLE IF NOT EXISTS fact_price_intelligence (
    intel_id INTEGER PRIMARY KEY,
    date_key INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    retail_shelf_price_kes DECIMAL(10, 2) NOT NULL,
    kamis_retail_benchmark_kes DECIMAL(10, 2) NOT NULL,
    spread_vs_market_kes DECIMAL(10, 2) NOT NULL,
    competitiveness_index_pct DECIMAL(6, 2) NOT NULL
);

-- ============================================================================
-- 3. PERFORMANCE INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pos_sales_date ON fact_pos_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_pos_sales_store ON fact_pos_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_pos_sales_product ON fact_pos_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_pos_sales_supplier ON fact_pos_sales(supplier_key);

CREATE INDEX IF NOT EXISTS idx_inv_store ON fact_inventory_snapshot(store_key);
CREATE INDEX IF NOT EXISTS idx_inv_product ON fact_inventory_snapshot(product_key);

CREATE INDEX IF NOT EXISTS idx_kamis_date ON fact_kamis_market_price(date_key);
CREATE INDEX IF NOT EXISTS idx_kamis_market ON fact_kamis_market_price(market_key);

CREATE INDEX IF NOT EXISTS idx_intel_store ON fact_price_intelligence(store_key);
CREATE INDEX IF NOT EXISTS idx_intel_product ON fact_price_intelligence(product_key);
