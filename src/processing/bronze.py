import hashlib
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


def build_bronze_layer(output_dir: Optional[Path] = None) -> Dict[str, Any]:
    target_dir = output_dir or settings.bronze_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Building Medallion Bronze Layer (Immutable Raw Staging)...")

    # 1. KAMIS Bronze
    kamis_raw = settings.raw_kamis_dir / "kamis_prices_master.csv"
    df_kamis = pd.read_csv(kamis_raw)
    df_kamis["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_kamis["_source_file"] = str(kamis_raw.name)
    kamis_bronze_path = target_dir / "bronze_kamis.parquet"
    df_kamis.to_parquet(kamis_bronze_path, index=False)

    # 2. KNBS Bronze
    knbs_raw = settings.raw_knbs_dir / "knbs_cpi_master.csv"
    df_knbs = pd.read_csv(knbs_raw)
    df_knbs["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_knbs["_source_file"] = str(knbs_raw.name)
    knbs_bronze_path = target_dir / "bronze_knbs.parquet"
    df_knbs.to_parquet(knbs_bronze_path, index=False)

    # 3. Operational Entities Bronze
    op_dir = settings.raw_operational_dir
    df_stores = pd.read_csv(op_dir / "stores.csv")
    df_stores["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_stores.to_parquet(target_dir / "bronze_stores.parquet", index=False)

    df_products = pd.read_csv(op_dir / "products.csv")
    df_products["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_products.to_parquet(target_dir / "bronze_products.parquet", index=False)

    df_suppliers = pd.read_csv(op_dir / "suppliers.csv")
    df_suppliers["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_suppliers.to_parquet(target_dir / "bronze_suppliers.parquet", index=False)

    df_pos = pd.read_csv(op_dir / "pos_transactions_master.csv")
    df_pos["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_pos.to_parquet(target_dir / "bronze_pos_sales.parquet", index=False)

    df_inv = pd.read_csv(op_dir / "inventory_movements_master.csv")
    df_inv["_bronze_ingested_at"] = datetime.now(timezone.utc)
    df_inv.to_parquet(target_dir / "bronze_inventory.parquet", index=False)

    summary = {
        "bronze_kamis_rows": len(df_kamis),
        "bronze_knbs_rows": len(df_knbs),
        "bronze_stores_rows": len(df_stores),
        "bronze_products_rows": len(df_products),
        "bronze_suppliers_rows": len(df_suppliers),
        "bronze_pos_rows": len(df_pos),
        "bronze_inventory_rows": len(df_inv),
    }
    logger.info("Bronze Layer successfully built: %d POS, %d KAMIS, %d Inv rows staged.",
                summary["bronze_pos_rows"], summary["bronze_kamis_rows"], summary["bronze_inventory_rows"])
    return summary
