from pathlib import Path
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@pytest.fixture
def sqlite_test_engine(tmp_path: Path) -> Engine:
    db_file = (tmp_path / "test_warehouse.db").as_posix()
    return create_engine(f"sqlite:///{db_file}")


@pytest.fixture
def sample_pos_record():
    return {
        "transaction_id": "TXN-TEST-0001",
        "receipt_number": "REC-000001",
        "timestamp": "2024-03-15T10:30:00+00:00",
        "store_id": "STR-001",
        "product_id": "PRD-001",
        "quantity": 2,
        "unit_price_kes": 200.0,
        "discount_kes": 0.0,
        "line_total_kes": 400.0,
        "payment_method": "M-Pesa",
        "channel": "Store POS",
    }


@pytest.fixture
def sample_inventory_record():
    return {
        "movement_id": "INV-TEST-0001",
        "timestamp": "2024-03-15T11:00:00+00:00",
        "store_id": "STR-001",
        "product_id": "PRD-001",
        "movement_type": "RESTOCK",
        "quantity_delta": 50,
        "stock_on_hand": 120,
        "reorder_point": 35,
        "is_below_reorder": False,
    }
