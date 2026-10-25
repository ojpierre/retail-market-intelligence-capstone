import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


def build_gold_layer(output_dir: Optional[Path] = None) -> Dict[str, Any]:
    target_dir = output_dir or settings.gold_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    silver_dir = settings.silver_dir

    logger.info("Building Medallion Gold Layer (Business Aggregations & Analytical Marts)...")

    df_pos = pd.read_parquet(silver_dir / "silver_pos_sales.parquet")
    df_inv = pd.read_parquet(silver_dir / "silver_inventory.parquet")
    df_kamis = pd.read_parquet(silver_dir / "silver_kamis_prices.parquet")

    # Mart 1: Daily Store Sales & Performance
    daily_store = df_pos.groupby(
        ["sale_date", "store_id", "store_name", "county", "region", "format"],
        as_index=False,
    ).agg(
        nominal_revenue_kes=("line_total_kes", "sum"),
        real_revenue_kes=("real_line_total_kes", "sum"),
        gross_profit_kes=("gross_profit_kes", "sum"),
        items_sold=("quantity", "sum"),
        transactions_count=("transaction_id", "nunique"),
    )
    daily_store["nominal_revenue_kes"] = daily_store["nominal_revenue_kes"].round(2)
    daily_store["real_revenue_kes"] = daily_store["real_revenue_kes"].round(2)
    daily_store["gross_profit_kes"] = daily_store["gross_profit_kes"].round(2)
    daily_store["gross_margin_pct"] = (
        (daily_store["gross_profit_kes"] / daily_store["nominal_revenue_kes"]) * 100.0
    ).round(2)
    daily_store["_gold_created_at"] = datetime.now(timezone.utc)
    daily_store.to_parquet(target_dir / "gold_daily_store_sales.parquet", index=False)

    # Mart 2: Price Competitiveness (Retailer vs KAMIS Benchmark)
    # Filter sales that have a KAMIS commodity mapping
    df_pos_mapped = df_pos.dropna(subset=["kamis_comm", "kamis_retail_benchmark_kes"]).copy()
    competitiveness = df_pos_mapped.groupby(
        ["county", "year", "month", "kamis_comm"],
        as_index=False,
    ).agg(
        avg_shelf_price_per_std_unit=("retail_price_per_std_unit_kes", "mean"),
        avg_kamis_retail_benchmark=("kamis_retail_benchmark_kes", "mean"),
        total_volume_sold=("quantity", "sum"),
        total_revenue_kes=("line_total_kes", "sum"),
    )
    competitiveness["avg_shelf_price_per_std_unit"] = competitiveness["avg_shelf_price_per_std_unit"].round(2)
    competitiveness["avg_kamis_retail_benchmark"] = competitiveness["avg_kamis_retail_benchmark"].round(2)
    competitiveness["shelf_vs_market_spread_kes"] = (
        competitiveness["avg_shelf_price_per_std_unit"] - competitiveness["avg_kamis_retail_benchmark"]
    ).round(2)
    competitiveness["competitiveness_index_pct"] = (
        (competitiveness["avg_shelf_price_per_std_unit"] / competitiveness["avg_kamis_retail_benchmark"]) * 100.0
    ).round(2)
    competitiveness["_gold_created_at"] = datetime.now(timezone.utc)
    competitiveness.to_parquet(target_dir / "gold_price_competitiveness.parquet", index=False)

    # Mart 3: Product Profitability & Margin Ranking
    product_marts = df_pos.groupby(
        ["product_id", "name", "category", "supplier_name"],
        as_index=False,
    ).agg(
        total_quantity_sold=("quantity", "sum"),
        total_revenue_kes=("line_total_kes", "sum"),
        total_gross_profit_kes=("gross_profit_kes", "sum"),
        avg_discount_given=("discount_kes", "mean"),
    )
    product_marts["total_revenue_kes"] = product_marts["total_revenue_kes"].round(2)
    product_marts["total_gross_profit_kes"] = product_marts["total_gross_profit_kes"].round(2)
    product_marts["profit_margin_pct"] = (
        (product_marts["total_gross_profit_kes"] / product_marts["total_revenue_kes"]) * 100.0
    ).round(2)
    product_marts["_gold_created_at"] = datetime.now(timezone.utc)
    product_marts.to_parquet(target_dir / "gold_product_profitability.parquet", index=False)

    # Mart 4: Inventory Risk & Stockout Volatility
    inv_risk = df_inv.groupby(
        ["store_id", "store_name", "county", "product_id", "name", "category"],
        as_index=False,
    ).agg(
        total_movements=("movement_id", "count"),
        latest_stock_on_hand=("stock_on_hand", "last"),
        stockout_incidents=("is_below_reorder", "sum"),
    )
    inv_risk["_gold_created_at"] = datetime.now(timezone.utc)
    inv_risk.to_parquet(target_dir / "gold_inventory_risk.parquet", index=False)

    summary = {
        "gold_daily_store_sales_rows": len(daily_store),
        "gold_price_competitiveness_rows": len(competitiveness),
        "gold_product_profitability_rows": len(product_marts),
        "gold_inventory_risk_rows": len(inv_risk),
    }
    logger.info("Gold Layer built successfully: 4 analytical marts populated.")
    return summary
