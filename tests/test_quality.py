import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from src.quality import DataQualityEngine
from src.warehouse import init_warehouse_schema, populate_dimensions, load_facts


def test_data_quality_assertions_pass(sqlite_test_engine: Engine):
    init_warehouse_schema(sqlite_test_engine)
    populate_dimensions(sqlite_test_engine)
    load_facts(sqlite_test_engine, mode="replace")

    dq = DataQualityEngine(sqlite_test_engine)
    all_passed, results = dq.run_all_assertions()

    assert all_passed is True
    assert len(results) == 8
    for r in results:
        assert r["passed"] is True


def test_audit_recording(sqlite_test_engine: Engine):
    init_warehouse_schema(sqlite_test_engine)
    populate_dimensions(sqlite_test_engine)
    load_facts(sqlite_test_engine, mode="replace")

    dq = DataQualityEngine(sqlite_test_engine)
    all_passed, results = dq.run_all_assertions()

    dq.record_audit(
        batch_id="TEST-BATCH-AUDIT",
        stage="test_stage",
        total_records=1000,
        quarantined_count=0,
        duration_sec=0.25,
        dq_results=results,
    )

    with sqlite_test_engine.connect() as conn:
        row = conn.execute(text("SELECT batch_id, status, total_checks FROM data_quality_audit_log WHERE batch_id = 'TEST-BATCH-AUDIT'")).fetchone()
        assert row is not None
        assert row[0] == "TEST-BATCH-AUDIT"
        assert row[1] == "PASSED"
        assert row[2] == 8
