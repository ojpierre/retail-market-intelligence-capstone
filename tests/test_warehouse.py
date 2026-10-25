from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from src.warehouse import init_warehouse_schema, populate_dimensions, load_facts


def test_warehouse_initialization(sqlite_test_engine: Engine):
    init_warehouse_schema(sqlite_test_engine)
    inspector = inspect(sqlite_test_engine)
    tables = inspector.get_table_names()

    assert "dim_date" in tables
    assert "dim_store" in tables
    assert "dim_product" in tables
    assert "dim_supplier" in tables
    assert "dim_kamis_market" in tables
    assert "dim_knbs_cpi" in tables
    assert "fact_pos_sales" in tables
    assert "fact_inventory_snapshot" in tables
    assert "fact_kamis_market_price" in tables
    assert "fact_price_intelligence" in tables


def test_warehouse_loading(sqlite_test_engine: Engine):
    init_warehouse_schema(sqlite_test_engine)
    dims = populate_dimensions(sqlite_test_engine)
    assert dims["stores"] == 15
    assert dims["products"] == 25

    facts = load_facts(sqlite_test_engine, mode="replace")
    assert facts["fact_pos_sales"] > 0
    assert facts["fact_inventory_snapshot"] > 0

    with sqlite_test_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_pos_sales")).scalar()
        assert count == facts["fact_pos_sales"]
