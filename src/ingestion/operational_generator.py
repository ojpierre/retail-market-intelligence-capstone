import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


def compute_file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


STORES_MASTER = [
    {"store_id": "STR-001", "store_name": "Tuzo Westlands Mall", "county": "Nairobi", "region": "Nairobi", "format": "Hypermarket", "sqm": 3500, "lat": -1.2675, "lon": 36.8115},
    {"store_id": "STR-002", "store_name": "Tuzo Nairobi CBD Central", "county": "Nairobi", "region": "Nairobi", "format": "Supermarket", "sqm": 1800, "lat": -1.2841, "lon": 36.8236},
    {"store_id": "STR-003", "store_name": "Tuzo Kasarani Hub", "county": "Nairobi", "region": "Nairobi", "format": "Supermarket", "sqm": 2200, "lat": -1.2217, "lon": 36.9014},
    {"store_id": "STR-004", "store_name": "Tuzo Nyali Beachwalk", "county": "Mombasa", "region": "Coast", "format": "Hypermarket", "sqm": 3200, "lat": -4.0253, "lon": 39.7139},
    {"store_id": "STR-005", "store_name": "Tuzo Mombasa Town", "county": "Mombasa", "region": "Coast", "format": "Supermarket", "sqm": 1600, "lat": -4.0547, "lon": 39.6636},
    {"store_id": "STR-006", "store_name": "Tuzo Kisumu Mega", "county": "Kisumu", "region": "Nyanza", "format": "Supermarket", "sqm": 2400, "lat": -0.1017, "lon": 34.7556},
    {"store_id": "STR-007", "store_name": "Tuzo Nakuru Midtown", "county": "Nakuru", "region": "Rift Valley", "format": "Supermarket", "sqm": 2000, "lat": -0.2833, "lon": 36.0667},
    {"store_id": "STR-008", "store_name": "Tuzo Eldoret Rupa Mall", "county": "Uasin Gishu", "region": "Rift Valley", "format": "Hypermarket", "sqm": 3000, "lat": 0.5204, "lon": 35.2698},
    {"store_id": "STR-009", "store_name": "Tuzo Nyeri Central", "county": "Nyeri", "region": "Central", "format": "Express", "sqm": 950, "lat": -0.4201, "lon": 36.9476},
    {"store_id": "STR-010", "store_name": "Tuzo Thika Makongeni", "county": "Kiambu", "region": "Central", "format": "Supermarket", "sqm": 1750, "lat": -1.0333, "lon": 37.0694},
    {"store_id": "STR-011", "store_name": "Tuzo Machakos Plaza", "county": "Machakos", "region": "Eastern", "format": "Supermarket", "sqm": 1500, "lat": -1.5177, "lon": 37.2634},
    {"store_id": "STR-012", "store_name": "Tuzo Meru Town Center", "county": "Meru", "region": "Eastern", "format": "Express", "sqm": 1100, "lat": 0.0515, "lon": 37.6499},
    {"store_id": "STR-013", "store_name": "Tuzo Naivasha Junction", "county": "Nakuru", "region": "Rift Valley", "format": "Supermarket", "sqm": 1650, "lat": -0.7172, "lon": 36.4310},
    {"store_id": "STR-014", "store_name": "Tuzo Kitale Commercial", "county": "Trans Nzoia", "region": "Rift Valley", "format": "Supermarket", "sqm": 1800, "lat": 1.0167, "lon": 35.0000},
    {"store_id": "STR-015", "store_name": "Tuzo Garissa Oasis", "county": "Garissa", "region": "North Eastern", "format": "Express", "sqm": 900, "lat": -0.4532, "lon": 39.6460},
]

SUPPLIERS_MASTER = [
    {"supplier_id": "SUP-01", "supplier_name": "Unga Farm Care Ltd", "category": "Flour & Milling", "lead_time_days": 2, "rating": 4.8},
    {"supplier_id": "SUP-02", "supplier_name": "BIDCO Africa Ltd", "category": "Oils & Detergents", "lead_time_days": 3, "rating": 4.9},
    {"supplier_id": "SUP-03", "supplier_name": "Kapa Oil Refineries", "category": "Oils & Baking", "lead_time_days": 3, "rating": 4.7},
    {"supplier_id": "SUP-04", "supplier_name": "Brookside Dairy Ltd", "category": "Dairy Products", "lead_time_days": 1, "rating": 4.9},
    {"supplier_id": "SUP-05", "supplier_name": "Broadway Bakery Ltd", "category": "Bakery", "lead_time_days": 1, "rating": 4.8},
    {"supplier_id": "SUP-06", "supplier_name": "New KCC Limited", "category": "Dairy Products", "lead_time_days": 2, "rating": 4.6},
    {"supplier_id": "SUP-07", "supplier_name": "Pwani Oil Products", "category": "Oils & Personal Care", "lead_time_days": 4, "rating": 4.5},
    {"supplier_id": "SUP-08", "supplier_name": "Cereal Growers Eldoret", "category": "Grains & Pulses", "lead_time_days": 4, "rating": 4.4},
    {"supplier_id": "SUP-09", "supplier_name": "Kinangop Potato Farmers Co-op", "category": "Tubers", "lead_time_days": 2, "rating": 4.6},
    {"supplier_id": "SUP-10", "supplier_name": "Mwea Rice Growers SACCO", "category": "Rice", "lead_time_days": 3, "rating": 4.7},
    {"supplier_id": "SUP-11", "supplier_name": "Mumias Sugar Distributors", "category": "Sugar", "lead_time_days": 5, "rating": 4.2},
    {"supplier_id": "SUP-12", "supplier_name": "Kensalt Salt Refineries", "category": "Salt & Condiments", "lead_time_days": 3, "rating": 4.8},
    {"supplier_id": "SUP-13", "supplier_name": "Naivasha Fresh Veg Aggregators", "category": "Vegetables", "lead_time_days": 1, "rating": 4.7},
    {"supplier_id": "SUP-14", "supplier_name": "Unilever Kenya Ltd", "category": "Home & Personal Care", "lead_time_days": 3, "rating": 4.9},
    {"supplier_id": "SUP-15", "supplier_name": "Colgate-Palmolive East Africa", "category": "Personal Care", "lead_time_days": 4, "rating": 4.8},
    {"supplier_id": "SUP-16", "supplier_name": "Kenya Tea Packers (KETEPA)", "category": "Beverages", "lead_time_days": 2, "rating": 4.9},
    {"supplier_id": "SUP-17", "supplier_name": "Capwell Industries Ltd", "category": "Flour & Porridge", "lead_time_days": 3, "rating": 4.7},
    {"supplier_id": "SUP-18", "supplier_name": "Farmers Choice Ltd", "category": "Processed Meats", "lead_time_days": 1, "rating": 4.8},
    {"supplier_id": "SUP-19", "supplier_name": "Crown Beverages Ltd", "category": "Beverages", "lead_time_days": 2, "rating": 4.5},
    {"supplier_id": "SUP-20", "supplier_name": "Highlands Drinks Ltd", "category": "Beverages & Water", "lead_time_days": 2, "rating": 4.6},
]

PRODUCTS_MASTER = [
    # Grains & Flours
    {"product_id": "PRD-001", "sku": "SKU-MZ-001", "name": "Jogoo Maize Flour 2kg", "category": "Flour & Grains", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 170.0, "rrp": 205.0, "kamis_comm": "White Maize", "supplier_id": "SUP-01"},
    {"product_id": "PRD-002", "sku": "SKU-MZ-002", "name": "Pembe Maize Flour 2kg", "category": "Flour & Grains", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 168.0, "rrp": 200.0, "kamis_comm": "White Maize", "supplier_id": "SUP-01"},
    {"product_id": "PRD-003", "sku": "SKU-MZ-003", "name": "Hostess Premium Maize Flour 2kg", "category": "Flour & Grains", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 210.0, "rrp": 255.0, "kamis_comm": "White Maize", "supplier_id": "SUP-01"},
    {"product_id": "PRD-004", "sku": "SKU-WF-001", "name": "Ajab Home Baking Wheat Flour 2kg", "category": "Flour & Grains", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 175.0, "rrp": 215.0, "kamis_comm": "Wheat Flour", "supplier_id": "SUP-17"},
    {"product_id": "PRD-005", "sku": "SKU-RC-001", "name": "Daawat Aromatic Rice 2kg", "category": "Rice", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 380.0, "rrp": 460.0, "kamis_comm": "Imported Rice", "supplier_id": "SUP-10"},
    {"product_id": "PRD-006", "sku": "SKU-RC-002", "name": "Mwea Pishori Pure Rice 2kg", "category": "Rice", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 410.0, "rrp": 510.0, "kamis_comm": "Imported Rice", "supplier_id": "SUP-10"},
    # Pulses & Tubers
    {"product_id": "PRD-007", "sku": "SKU-BN-001", "name": "Pearl Rose Coco Beans 1kg", "category": "Pulses", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 140.0, "rrp": 175.0, "kamis_comm": "Rose Coco Beans", "supplier_id": "SUP-08"},
    {"product_id": "PRD-008", "sku": "SKU-BN-002", "name": "Pearl Yellow Beans 1kg", "category": "Pulses", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 155.0, "rrp": 195.0, "kamis_comm": "Rose Coco Beans", "supplier_id": "SUP-08"},
    {"product_id": "PRD-009", "sku": "SKU-PT-001", "name": "Fresh Irish Potatoes Net Bag 2kg", "category": "Tubers", "unit_size": "2 KG", "std_unit": "KG", "unit_factor": 2.0, "cost": 120.0, "rrp": 160.0, "kamis_comm": "Irish Potatoes", "supplier_id": "SUP-09"},
    # Cooking Oils & Fats
    {"product_id": "PRD-010", "sku": "SKU-OL-001", "name": "Elianto Pure Corn Oil 1L", "category": "Oils & Fats", "unit_size": "1 L", "std_unit": "L", "unit_factor": 1.0, "cost": 370.0, "rrp": 435.0, "kamis_comm": "Cooking Oil", "supplier_id": "SUP-02"},
    {"product_id": "PRD-011", "sku": "SKU-OL-002", "name": "Golden Fry Cooking Oil 2L", "category": "Oils & Fats", "unit_size": "2 L", "std_unit": "L", "unit_factor": 2.0, "cost": 590.0, "rrp": 710.0, "kamis_comm": "Cooking Oil", "supplier_id": "SUP-02"},
    {"product_id": "PRD-012", "sku": "SKU-OL-003", "name": "Rina Vegetable Oil 1L", "category": "Oils & Fats", "unit_size": "1 L", "std_unit": "L", "unit_factor": 1.0, "cost": 310.0, "rrp": 365.0, "kamis_comm": "Cooking Oil", "supplier_id": "SUP-03"},
    {"product_id": "PRD-013", "sku": "SKU-OL-004", "name": "Kasuku Cooking Fat 1kg", "category": "Oils & Fats", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 290.0, "rrp": 345.0, "kamis_comm": "Cooking Oil", "supplier_id": "SUP-03"},
    # Dairy & Bakery
    {"product_id": "PRD-014", "sku": "SKU-MK-001", "name": "Brookside Fresh Milk 500ml", "category": "Dairy", "unit_size": "500 ML", "std_unit": "L", "unit_factor": 0.5, "cost": 50.0, "rrp": 65.0, "kamis_comm": "Cow Milk", "supplier_id": "SUP-04"},
    {"product_id": "PRD-015", "sku": "SKU-MK-002", "name": "KCC Homogenized Fresh Milk 500ml", "category": "Dairy", "unit_size": "500 ML", "std_unit": "L", "unit_factor": 0.5, "cost": 48.0, "rrp": 60.0, "kamis_comm": "Cow Milk", "supplier_id": "SUP-06"},
    {"product_id": "PRD-016", "sku": "SKU-MK-003", "name": "Ilara Long Life Milk 500ml", "category": "Dairy", "unit_size": "500 ML", "std_unit": "L", "unit_factor": 0.5, "cost": 52.0, "rrp": 65.0, "kamis_comm": "Cow Milk", "supplier_id": "SUP-04"},
    {"product_id": "PRD-017", "sku": "SKU-BD-001", "name": "Broadways Premium White Bread 400g", "category": "Bakery", "unit_size": "400 G", "std_unit": "KG", "unit_factor": 0.4, "cost": 55.0, "rrp": 70.0, "kamis_comm": None, "supplier_id": "SUP-05"},
    {"product_id": "PRD-018", "sku": "SKU-BD-002", "name": "Festive White Sliced Bread 400g", "category": "Bakery", "unit_size": "400 G", "std_unit": "KG", "unit_factor": 0.4, "cost": 53.0, "rrp": 65.0, "kamis_comm": None, "supplier_id": "SUP-05"},
    # Fresh Produce
    {"product_id": "PRD-019", "sku": "SKU-VG-001", "name": "Local Cabbage Head Medium", "category": "Fresh Produce", "unit_size": "1 PC", "std_unit": "KG", "unit_factor": 1.5, "cost": 45.0, "rrp": 65.0, "kamis_comm": "Cabbage", "supplier_id": "SUP-13"},
    {"product_id": "PRD-020", "sku": "SKU-VG-002", "name": "Fresh Salad Tomatoes 1kg Punnet", "category": "Fresh Produce", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 85.0, "rrp": 120.0, "kamis_comm": "Tomatoes", "supplier_id": "SUP-13"},
    {"product_id": "PRD-021", "sku": "SKU-VG-003", "name": "Red Dry Onions 1kg Mesh", "category": "Fresh Produce", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 105.0, "rrp": 140.0, "kamis_comm": "Red Onions", "supplier_id": "SUP-13"},
    # Essentials & Household
    {"product_id": "PRD-022", "sku": "SKU-SG-001", "name": "Mumias White Sugar 1kg Pack", "category": "Sugar & Sweeteners", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 155.0, "rrp": 185.0, "kamis_comm": None, "supplier_id": "SUP-11"},
    {"product_id": "PRD-023", "sku": "SKU-SL-001", "name": "Kensalt Iodised Table Salt 1kg", "category": "Pantry Essentials", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 32.0, "rrp": 45.0, "kamis_comm": None, "supplier_id": "SUP-12"},
    {"product_id": "PRD-024", "sku": "SKU-DT-001", "name": "Omo Multi-Active Washing Powder 1kg", "category": "Household & Laundry", "unit_size": "1 KG", "std_unit": "KG", "unit_factor": 1.0, "cost": 310.0, "rrp": 385.0, "kamis_comm": None, "supplier_id": "SUP-14"},
    {"product_id": "PRD-025", "sku": "SKU-DT-002", "name": "Sunlight Lemon Dishwashing Liquid 750ml", "category": "Household & Laundry", "unit_size": "750 ML", "std_unit": "L", "unit_factor": 0.75, "cost": 180.0, "rrp": 235.0, "kamis_comm": None, "supplier_id": "SUP-14"},
]


def generate_operational_datasets(
    num_transactions: int = 25000,
    num_inventory_events: int = 10000,
    start_year: int = 2023,
    end_year: int = 2024,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(42)

    df_stores = pd.DataFrame(STORES_MASTER)
    df_suppliers = pd.DataFrame(SUPPLIERS_MASTER)
    df_products = pd.DataFrame(PRODUCTS_MASTER)

    # 1. Point of Sale Transactions
    logger.info("Synthesizing %d POS retail transactions...", num_transactions)
    store_ids = df_stores["store_id"].values
    store_weights = [0.12, 0.10, 0.08, 0.09, 0.07, 0.08, 0.07, 0.08, 0.05, 0.06, 0.05, 0.04, 0.05, 0.04, 0.02]

    # Date generation
    start_dt = datetime(start_year, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(end_year, 12, 31, 21, 0, 0, tzinfo=timezone.utc)
    total_seconds = int((end_dt - start_dt).total_seconds())

    random_seconds = rng.integers(0, total_seconds, size=num_transactions)
    timestamps = [start_dt + timedelta(seconds=int(s)) for s in random_seconds]
    timestamps.sort()

    chosen_stores = rng.choice(store_ids, size=num_transactions, p=store_weights)
    prod_indices = rng.choice(len(df_products), size=num_transactions)
    quantities = rng.choice([1, 2, 3, 4, 5], size=num_transactions, p=[0.55, 0.25, 0.12, 0.05, 0.03])
    pay_methods = rng.choice(["M-Pesa", "Cash", "Card"], size=num_transactions, p=[0.68, 0.22, 0.10])

    pos_records: List[Dict[str, Any]] = []
    receipt_counter = 100000

    for i in range(num_transactions):
        p_row = df_products.iloc[prod_indices[i]]
        st_id = chosen_stores[i]
        ts = timestamps[i]
        qty = int(quantities[i])
        unit_price = float(p_row["rrp"])

        # Occasional promotional discount
        has_promo = (rng.random() < 0.08)
        discount = round(unit_price * qty * 0.10, 2) if has_promo else 0.0
        line_total = round(unit_price * qty - discount, 2)

        if i % 3 == 0:
            receipt_counter += 1

        pos_records.append({
            "transaction_id": f"TXN-{ts.strftime('%Y%m')}-{i+1:06d}",
            "receipt_number": f"REC-{receipt_counter:07d}",
            "timestamp": ts.isoformat(),
            "store_id": st_id,
            "product_id": p_row["product_id"],
            "quantity": qty,
            "unit_price_kes": unit_price,
            "discount_kes": discount,
            "line_total_kes": line_total,
            "payment_method": pay_methods[i],
            "channel": "Store POS",
        })

    df_pos = pd.DataFrame(pos_records)

    # 2. Inventory Movements
    logger.info("Synthesizing %d inventory movement logs...", num_inventory_events)
    inv_timestamps = [start_dt + timedelta(seconds=int(s)) for s in rng.integers(0, total_seconds, size=num_inventory_events)]
    inv_timestamps.sort()
    inv_stores = rng.choice(store_ids, size=num_inventory_events)
    inv_prod_idx = rng.choice(len(df_products), size=num_inventory_events)
    mv_types = rng.choice(["RESTOCK", "SALE_DISPATCH", "SHRINKAGE", "SUPPLIER_RETURN"], size=num_inventory_events, p=[0.40, 0.48, 0.07, 0.05])

    inv_records: List[Dict[str, Any]] = []
    # Track local stock
    stock_tracker: Dict[Tuple[str, str], int] = {}

    for i in range(num_inventory_events):
        st_id = inv_stores[i]
        p_row = df_products.iloc[inv_prod_idx[i]]
        pr_id = p_row["product_id"]
        m_type = mv_types[i]
        ts = inv_timestamps[i]

        curr_stock = stock_tracker.get((st_id, pr_id), rng.integers(100, 300))

        if m_type == "RESTOCK":
            qty_delta = int(rng.integers(50, 200))
            new_stock = curr_stock + qty_delta
        elif m_type == "SALE_DISPATCH":
            qty_delta = -int(rng.integers(5, 30))
            new_stock = max(0, curr_stock + qty_delta)
        elif m_type == "SHRINKAGE":
            qty_delta = -int(rng.integers(1, 5))
            new_stock = max(0, curr_stock + qty_delta)
        else:
            qty_delta = -int(rng.integers(2, 10))
            new_stock = max(0, curr_stock + qty_delta)

        stock_tracker[(st_id, pr_id)] = new_stock

        inv_records.append({
            "movement_id": f"INV-{ts.strftime('%Y%m')}-{i+1:06d}",
            "timestamp": ts.isoformat(),
            "store_id": st_id,
            "product_id": pr_id,
            "movement_type": m_type,
            "quantity_delta": qty_delta,
            "stock_on_hand": new_stock,
            "reorder_point": 35,
            "is_below_reorder": new_stock < 35,
        })

    df_inv = pd.DataFrame(inv_records)

    return df_stores, df_suppliers, df_products, df_pos, df_inv


def ingest_operational_data(
    output_dir: Optional[Path] = None,
    force: bool = False,
) -> Tuple[Path, Dict[str, Any]]:
    target_dir = output_dir or settings.raw_operational_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    stores_path = target_dir / "stores.csv"
    suppliers_path = target_dir / "suppliers.csv"
    products_path = target_dir / "products.csv"
    pos_path = target_dir / "pos_transactions_master.csv"
    inv_path = target_dir / "inventory_movements_master.csv"

    metadata: Dict[str, Any] = {
        "source": "Retail Chain Operational Datasets (POS, Inventory, Entities)",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "stores_count": 0,
        "suppliers_count": 0,
        "products_count": 0,
        "pos_count": 0,
        "inventory_count": 0,
    }

    if pos_path.exists() and not force:
        logger.info("Existing operational datasets found. Loading.")
        df_stores = pd.read_csv(stores_path)
        df_suppliers = pd.read_csv(suppliers_path)
        df_products = pd.read_csv(products_path)
        df_pos = pd.read_csv(pos_path)
        df_inv = pd.read_csv(inv_path)
    else:
        df_stores, df_suppliers, df_products, df_pos, df_inv = generate_operational_datasets()
        df_stores.to_csv(stores_path, index=False)
        df_suppliers.to_csv(suppliers_path, index=False)
        df_products.to_csv(products_path, index=False)
        df_pos.to_csv(pos_path, index=False)
        df_inv.to_csv(inv_path, index=False)

    metadata["stores_count"] = len(df_stores)
    metadata["suppliers_count"] = len(df_suppliers)
    metadata["products_count"] = len(df_products)
    metadata["pos_count"] = len(df_pos)
    metadata["inventory_count"] = len(df_inv)
    metadata["pos_checksum_sha256"] = compute_file_sha256(pos_path)

    # Partition transactions by year and month
    df_pos["_dt"] = pd.to_datetime(df_pos["timestamp"])
    for (year, month), part_df in df_pos.groupby([df_pos["_dt"].dt.year, df_pos["_dt"].dt.month]):
        part_dir = target_dir / "transactions" / f"year={int(year)}" / f"month={int(month):02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_df.drop(columns=["_dt"]).to_csv(part_dir / "transactions_batch.csv", index=False)

    logger.info(
        "Operational Ingestion complete: %d Stores, %d Products, %d POS Txns, %d Inv logs",
        len(df_stores), len(df_products), len(df_pos), len(df_inv)
    )
    return target_dir, metadata
