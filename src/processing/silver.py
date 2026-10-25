import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)

KAMIS_UNIT_DIVISORS = {
    "90 KG bag": 90.0,
    "50 KG bag": 50.0,
    "100 KG bag": 100.0,
    "64 KG crate": 64.0,
    "50 KG net": 50.0,
    "24 KG bale": 24.0,
    "20 L jerrican": 20.0,
    "50 L can": 50.0,
}


def build_silver_layer(output_dir: Optional[Path] = None) -> Dict[str, Any]:
    target_dir = output_dir or settings.silver_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    bronze_dir = settings.bronze_dir

    logger.info("Building Medallion Silver Layer (Cleaned, Normalised, and Enriched)...")

    # 1. Silver KAMIS Prices
    df_kamis = pd.read_parquet(bronze_dir / "bronze_kamis.parquet")
    # Unit standardisation
    df_kamis["divisor"] = df_kamis["raw_unit"].map(KAMIS_UNIT_DIVISORS).fillna(1.0)
    df_kamis["norm_wholesale_price_kes"] = (df_kamis["wholesale_price_kes"] / df_kamis["divisor"]).round(2)
    df_kamis["norm_retail_price_kes"] = df_kamis["retail_price_kes"].round(2)
    df_kamis["wholesale_retail_spread_kes"] = (df_kamis["norm_retail_price_kes"] - df_kamis["norm_wholesale_price_kes"]).round(2)
    df_kamis["wholesale_markup_pct"] = (
        (df_kamis["norm_retail_price_kes"] - df_kamis["norm_wholesale_price_kes"]) / df_kamis["norm_wholesale_price_kes"] * 100.0
    ).round(2)
    df_kamis["_silver_processed_at"] = datetime.now(timezone.utc)
    df_kamis.to_parquet(target_dir / "silver_kamis_prices.parquet", index=False)

    # 2. Silver KNBS Indices
    df_knbs = pd.read_parquet(bronze_dir / "bronze_knbs.parquet")
    df_knbs["_silver_processed_at"] = datetime.now(timezone.utc)
    df_knbs.to_parquet(target_dir / "silver_knbs.parquet", index=False)

    # 3. Reference Dimensions
    df_stores = pd.read_parquet(bronze_dir / "bronze_stores.parquet")
    df_products = pd.read_parquet(bronze_dir / "bronze_products.parquet")
    df_suppliers = pd.read_parquet(bronze_dir / "bronze_suppliers.parquet")

    # 4. Silver POS Sales Enriched
    df_pos = pd.read_parquet(bronze_dir / "bronze_pos_sales.parquet")
    initial_pos_count = len(df_pos)

    # Deduplication
    df_pos = df_pos.drop_duplicates(subset=["transaction_id"])
    df_pos = df_pos.dropna(subset=["transaction_id", "store_id", "product_id", "quantity", "unit_price_kes"])
    df_pos = df_pos[df_pos["unit_price_kes"] > 0]

    # Date parsing
    df_pos["timestamp_dt"] = pd.to_datetime(df_pos["timestamp"])
    df_pos["sale_date"] = df_pos["timestamp_dt"].dt.date
    df_pos["year"] = df_pos["timestamp_dt"].dt.year
    df_pos["month"] = df_pos["timestamp_dt"].dt.month

    # Join Store metadata
    df_pos = df_pos.merge(
        df_stores[["store_id", "store_name", "county", "region", "format"]],
        on="store_id",
        how="left",
    )

    # Join Product & Supplier metadata
    df_pos = df_pos.merge(
        df_products[["product_id", "name", "category", "unit_size", "std_unit", "unit_factor", "cost", "kamis_comm", "supplier_id"]],
        on="product_id",
        how="left",
    )
    df_pos = df_pos.merge(
        df_suppliers[["supplier_id", "supplier_name"]],
        on="supplier_id",
        how="left",
    )

    # Unit price per standard unit (e.g. per 1 KG or 1 L)
    df_pos["unit_factor"] = df_pos["unit_factor"].fillna(1.0)
    df_pos["retail_price_per_std_unit_kes"] = (df_pos["unit_price_kes"] / df_pos["unit_factor"]).round(2)

    # Gross Profit Margins
    df_pos["cost_price_kes"] = df_pos["cost"]
    df_pos["total_cost_kes"] = (df_pos["cost_price_kes"] * df_pos["quantity"]).round(2)
    df_pos["gross_profit_kes"] = (df_pos["line_total_kes"] - df_pos["total_cost_kes"]).round(2)
    df_pos["gross_margin_pct"] = (
        (df_pos["gross_profit_kes"] / df_pos["line_total_kes"]) * 100.0
    ).round(2)

    # Deflate Sales using KNBS Food CPI (Base 2019=100)
    df_pos = df_pos.merge(
        df_knbs[["year", "month", "cpi_overall", "cpi_food", "food_inflation_yoy"]],
        on=["year", "month", "month"],
        how="left",
    )
    df_pos["cpi_food"] = df_pos["cpi_food"].fillna(150.0)
    df_pos["real_line_total_kes"] = ((df_pos["line_total_kes"] / df_pos["cpi_food"]) * 100.0).round(2)

    # Join KAMIS County-Level Retail Benchmark
    # Match on county, month, and kamis_comm
    df_kamis["_dt"] = pd.to_datetime(df_kamis["date"])
    df_kamis["year"] = df_kamis["_dt"].dt.year
    df_kamis["month"] = df_kamis["_dt"].dt.month

    kamis_lookup = df_kamis.groupby(["county", "year", "month", "commodity"])["norm_retail_price_kes"].mean().reset_index()
    kamis_lookup = kamis_lookup.rename(columns={"commodity": "kamis_comm", "norm_retail_price_kes": "kamis_retail_benchmark_kes"})

    df_pos = df_pos.merge(kamis_lookup, on=["county", "year", "month", "kamis_comm"], how="left")
    df_pos["price_diff_vs_market_kes"] = (df_pos["retail_price_per_std_unit_kes"] - df_pos["kamis_retail_benchmark_kes"]).round(2)
    df_pos["price_competitiveness_index"] = (
        (df_pos["retail_price_per_std_unit_kes"] / df_pos["kamis_retail_benchmark_kes"]) * 100.0
    ).round(2)

    df_pos["_silver_processed_at"] = datetime.now(timezone.utc)
    df_pos.to_parquet(target_dir / "silver_pos_sales.parquet", index=False)

    # 5. Silver Inventory Movements
    df_inv = pd.read_parquet(bronze_dir / "bronze_inventory.parquet")
    df_inv = df_inv.drop_duplicates(subset=["movement_id"])
    df_inv["timestamp_dt"] = pd.to_datetime(df_inv["timestamp"])
    df_inv["movement_date"] = df_inv["timestamp_dt"].dt.date
    df_inv = df_inv.merge(df_stores[["store_id", "store_name", "county"]], on="store_id", how="left")
    df_inv = df_inv.merge(df_products[["product_id", "name", "category"]], on="product_id", how="left")
    df_inv["_silver_processed_at"] = datetime.now(timezone.utc)
    df_inv.to_parquet(target_dir / "silver_inventory.parquet", index=False)

    summary = {
        "silver_kamis_rows": len(df_kamis),
        "silver_knbs_rows": len(df_knbs),
        "silver_pos_rows": len(df_pos),
        "silver_inventory_rows": len(df_inv),
        "deduplicated_pos_diff": initial_pos_count - len(df_pos),
    }
    logger.info("Silver Layer complete: %d POS transactions enriched with KNBS deflation and KAMIS benchmarks.", len(df_pos))
    return summary
