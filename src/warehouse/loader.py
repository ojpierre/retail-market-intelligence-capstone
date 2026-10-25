import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


def init_warehouse_schema(engine: Engine) -> None:
    ddl_file = settings.base_dir / "sql" / "ddl_schema.sql"
    with open(ddl_file, "r", encoding="utf-8") as f:
        ddl_text = f.read()

    statements = [stmt.strip() for stmt in ddl_text.split(";") if stmt.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    logger.info("Warehouse Star Schema DDL initialized with %d statements.", len(statements))


def populate_dimensions(engine: Engine) -> Dict[str, int]:
    silver_dir = settings.silver_dir
    bronze_dir = settings.bronze_dir

    # 1. dim_date
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
    dim_date_records = []
    for dt in dates:
        dim_date_records.append({
            "date_key": int(dt.strftime("%Y%m%d")),
            "full_date": dt.date(),
            "year": dt.year,
            "quarter": dt.quarter,
            "month": dt.month,
            "month_name": dt.strftime("%B"),
            "day_of_week": dt.strftime("%A"),
            "is_weekend": dt.weekday() >= 5,
            "is_harvest_season": dt.month in [10, 11, 12],
        })
    df_dim_date = pd.DataFrame(dim_date_records)

    # 2. dim_store
    df_stores = pd.read_parquet(bronze_dir / "bronze_stores.parquet")
    df_dim_store = df_stores.rename(columns={"format": "store_format", "sqm": "sqm_size", "lat": "latitude", "lon": "longitude"}).copy()
    df_dim_store = df_dim_store.sort_values(by="store_id")
    df_dim_store["store_key"] = range(1, len(df_dim_store) + 1)
    df_dim_store = df_dim_store[[
        "store_key", "store_id", "store_name", "county", "region",
        "store_format", "sqm_size", "latitude", "longitude"
    ]]

    # 3. dim_product
    df_products = pd.read_parquet(bronze_dir / "bronze_products.parquet")
    df_dim_product = df_products.rename(columns={
        "sku": "sku_code",
        "name": "product_name",
        "std_unit": "standard_unit",
        "cost": "cost_price_kes",
        "rrp": "recommended_retail_price_kes",
        "kamis_comm": "kamis_commodity_match",
    }).copy()
    df_dim_product = df_dim_product.sort_values(by="product_id")
    df_dim_product["product_key"] = range(1, len(df_dim_product) + 1)
    df_dim_product = df_dim_product[[
        "product_key", "product_id", "sku_code", "product_name", "category",
        "unit_size", "standard_unit", "cost_price_kes", "recommended_retail_price_kes",
        "kamis_commodity_match"
    ]]

    # 4. dim_supplier
    df_suppliers = pd.read_parquet(bronze_dir / "bronze_suppliers.parquet")
    df_dim_supplier = df_suppliers.rename(columns={"category": "category_focus"}).copy()
    df_dim_supplier = df_dim_supplier.sort_values(by="supplier_id")
    df_dim_supplier["supplier_key"] = range(1, len(df_dim_supplier) + 1)
    df_dim_supplier = df_dim_supplier[[
        "supplier_key", "supplier_id", "supplier_name", "category_focus",
        "lead_time_days", "rating"
    ]]

    # 5. dim_kamis_market
    df_kamis = pd.read_parquet(silver_dir / "silver_kamis_prices.parquet")
    df_markets = df_kamis[["market_name", "county", "region", "latitude", "longitude"]].drop_duplicates(subset=["market_name"]).copy()
    df_markets = df_markets.sort_values(by="market_name")
    df_markets["market_key"] = range(1, len(df_markets) + 1)
    df_markets = df_markets[[
        "market_key", "market_name", "county", "region", "latitude", "longitude"
    ]]

    # 6. dim_knbs_cpi
    df_knbs = pd.read_parquet(silver_dir / "silver_knbs.parquet")
    df_knbs = df_knbs.sort_values(by=["year", "month"]).copy()
    df_knbs["cpi_key"] = range(1, len(df_knbs) + 1)
    df_knbs_dim = df_knbs[[
        "cpi_key", "year", "month", "cpi_overall", "cpi_food", "cpi_housing_fuel",
        "inflation_rate_yoy", "food_inflation_yoy"
    ]]

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_date;"))
        df_dim_date.to_sql("dim_date", con=conn, if_exists="append", index=False, chunksize=1000)

        conn.execute(text("DELETE FROM dim_store;"))
        df_dim_store.to_sql("dim_store", con=conn, if_exists="append", index=False)

        conn.execute(text("DELETE FROM dim_product;"))
        df_dim_product.to_sql("dim_product", con=conn, if_exists="append", index=False)

        conn.execute(text("DELETE FROM dim_supplier;"))
        df_dim_supplier.to_sql("dim_supplier", con=conn, if_exists="append", index=False)

        conn.execute(text("DELETE FROM dim_kamis_market;"))
        df_markets.to_sql("dim_kamis_market", con=conn, if_exists="append", index=False)

        conn.execute(text("DELETE FROM dim_knbs_cpi;"))
        df_knbs_dim.to_sql("dim_knbs_cpi", con=conn, if_exists="append", index=False)

    logger.info("Conformed dimensions populated successfully.")
    return {
        "dates": len(df_dim_date),
        "stores": len(df_dim_store),
        "products": len(df_dim_product),
        "suppliers": len(df_dim_supplier),
        "markets": len(df_markets),
        "cpi": len(df_knbs_dim),
    }


def load_facts(engine: Engine, mode: str = "replace") -> Dict[str, int]:
    silver_dir = settings.silver_dir

    with engine.connect() as conn:
        dim_store = pd.read_sql("SELECT store_key, store_id FROM dim_store", conn)
        dim_prod = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
        dim_supp = pd.read_sql("SELECT supplier_key, supplier_id FROM dim_supplier", conn)
        dim_market = pd.read_sql("SELECT market_key, market_name FROM dim_kamis_market", conn)

    # 1. Fact POS Sales
    df_pos = pd.read_parquet(silver_dir / "silver_pos_sales.parquet")
    df_pos["date_key"] = pd.to_datetime(df_pos["sale_date"]).dt.strftime("%Y%m%d").astype(int)

    # Key lookups
    df_pos = df_pos.merge(dim_store, on="store_id", how="left")
    df_pos = df_pos.merge(dim_prod, on="product_id", how="left")
    df_pos = df_pos.merge(dim_supp, on="supplier_id", how="left")

    df_pos["sales_id"] = range(1, len(df_pos) + 1)
    df_pos["ingested_at"] = datetime.now(timezone.utc)

    fact_pos_cols = [
        "sales_id", "date_key", "store_key", "product_key", "supplier_key",
        "transaction_id", "receipt_number", "payment_method", "quantity",
        "unit_price_kes", "discount_kes", "nominal_revenue_kes", "real_revenue_kes",
        "cost_price_kes", "total_cost_kes", "gross_profit_kes", "gross_margin_pct",
        "ingested_at",
    ]
    df_pos_fact = df_pos.rename(columns={"line_total_kes": "nominal_revenue_kes", "real_line_total_kes": "real_revenue_kes"})[fact_pos_cols].copy()

    # 2. Fact Inventory Snapshot
    df_inv = pd.read_parquet(silver_dir / "silver_inventory.parquet")
    df_inv["date_key"] = pd.to_datetime(df_inv["movement_date"]).dt.strftime("%Y%m%d").astype(int)
    df_inv = df_inv.merge(dim_store, on="store_id", how="left")
    df_inv = df_inv.merge(dim_prod, on="product_id", how="left")
    df_inv = df_inv.rename(columns={"reorder_point": "reorder_threshold"})
    df_inv["snapshot_id"] = range(1, len(df_inv) + 1)
    df_inv["recorded_at"] = datetime.now(timezone.utc)

    fact_inv_cols = [
        "snapshot_id", "date_key", "store_key", "product_key", "movement_type",
        "quantity_delta", "stock_on_hand", "reorder_threshold", "is_below_reorder",
        "recorded_at",
    ]
    df_inv_fact = df_inv[fact_inv_cols].copy()

    # 3. Fact KAMIS Market Price
    df_kamis = pd.read_parquet(silver_dir / "silver_kamis_prices.parquet")
    df_kamis["date_key"] = pd.to_datetime(df_kamis["date"]).dt.strftime("%Y%m%d").astype(int)
    df_kamis = df_kamis.merge(dim_market, on="market_name", how="left")
    df_kamis["price_id"] = range(1, len(df_kamis) + 1)

    fact_kamis_cols = [
        "price_id", "date_key", "market_key", "commodity", "category",
        "raw_unit", "std_unit", "wholesale_price_kes", "norm_wholesale_price_kes",
        "norm_retail_price_kes", "wholesale_retail_spread_kes", "wholesale_markup_pct",
    ]
    df_kamis_fact = df_kamis[fact_kamis_cols].rename(columns={"commodity": "commodity_name"}).copy()

    # 4. Fact Price Intelligence
    df_intel = df_pos.dropna(subset=["kamis_retail_benchmark_kes"]).copy()
    df_intel["intel_id"] = range(1, len(df_intel) + 1)
    df_intel["retail_shelf_price_kes"] = df_intel["retail_price_per_std_unit_kes"]
    df_intel["spread_vs_market_kes"] = df_intel["price_diff_vs_market_kes"]
    df_intel["competitiveness_index_pct"] = df_intel["price_competitiveness_index"]

    fact_intel_cols = [
        "intel_id", "date_key", "store_key", "product_key",
        "retail_shelf_price_kes", "kamis_retail_benchmark_kes",
        "spread_vs_market_kes", "competitiveness_index_pct",
    ]
    df_intel_fact = df_intel[fact_intel_cols].copy()

    # Load all fact tables into database
    with engine.begin() as conn:
        if mode == "replace":
            conn.execute(text("DELETE FROM fact_pos_sales;"))
            conn.execute(text("DELETE FROM fact_inventory_snapshot;"))
            conn.execute(text("DELETE FROM fact_kamis_market_price;"))
            conn.execute(text("DELETE FROM fact_price_intelligence;"))

        df_pos_fact.to_sql("fact_pos_sales", con=conn, if_exists="append", index=False, chunksize=1000)
        df_inv_fact.to_sql("fact_inventory_snapshot", con=conn, if_exists="append", index=False, chunksize=1000)
        df_kamis_fact.to_sql("fact_kamis_market_price", con=conn, if_exists="append", index=False, chunksize=1000)
        df_intel_fact.to_sql("fact_price_intelligence", con=conn, if_exists="append", index=False, chunksize=1000)

    summary = {
        "fact_pos_sales": len(df_pos_fact),
        "fact_inventory_snapshot": len(df_inv_fact),
        "fact_kamis_market_price": len(df_kamis_fact),
        "fact_price_intelligence": len(df_intel_fact),
    }
    logger.info("Fact tables loaded: %d POS Sales, %d Inv, %d KAMIS, %d Intel",
                summary["fact_pos_sales"], summary["fact_inventory_snapshot"],
                summary["fact_kamis_market_price"], summary["fact_price_intelligence"])
    return summary
