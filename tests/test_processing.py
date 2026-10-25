from pathlib import Path
from src.processing import build_bronze_layer, build_silver_layer, build_gold_layer


def test_medallion_processing_flow(tmp_path: Path):
    bronze = build_bronze_layer()
    assert bronze["bronze_pos_rows"] >= 100
    assert bronze["bronze_kamis_rows"] == 2400

    silver = build_silver_layer()
    assert silver["silver_pos_rows"] >= 100
    assert silver["silver_kamis_rows"] == 2400

    gold = build_gold_layer()
    assert gold["gold_daily_store_sales_rows"] > 0
    assert gold["gold_price_competitiveness_rows"] > 0
    assert gold["gold_product_profitability_rows"] == 25
