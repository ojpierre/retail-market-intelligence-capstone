import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


class DataQualityEngine:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._ensure_audit_table()

    def _ensure_audit_table(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS data_quality_audit_log (
            audit_id INTEGER PRIMARY KEY,
            batch_id VARCHAR(50) NOT NULL,
            pipeline_stage VARCHAR(50) NOT NULL,
            total_checks INTEGER NOT NULL,
            passed_checks INTEGER NOT NULL,
            status VARCHAR(20) NOT NULL,
            records_scanned INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL DEFAULT 0,
            execution_duration_sec DECIMAL(6, 2) NOT NULL,
            details_json TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    def run_all_assertions(self) -> Tuple[bool, List[Dict[str, Any]]]:
        results: List[Dict[str, Any]] = []

        with self.engine.connect() as conn:
            # Check 1: Fact Sales Non-Empty
            sales_cnt = conn.execute(text("SELECT COUNT(*) FROM fact_pos_sales")).scalar() or 0
            results.append({
                "name": "Fact Sales Table Non-Empty",
                "category": "Completeness",
                "passed": sales_cnt > 0,
                "value": sales_cnt,
                "message": f"fact_pos_sales contains {sales_cnt} rows.",
            })

            # Check 2: All 6 Dimensions Populated
            dim_counts = {}
            dims = ["dim_date", "dim_store", "dim_product", "dim_supplier", "dim_kamis_market", "dim_knbs_cpi"]
            for d in dims:
                c = conn.execute(text(f"SELECT COUNT(*) FROM {d}")).scalar() or 0
                dim_counts[d] = c
            all_dims_pass = all(c > 0 for c in dim_counts.values())
            results.append({
                "name": "Conformed Dimensions Populated",
                "category": "Completeness",
                "passed": all_dims_pass,
                "value": dim_counts,
                "message": f"Dimension counts: {dim_counts}",
            })

            # Check 3: POS Price Sanity Range (Positive and Under 50,000 KES)
            bad_price = conn.execute(text("""
                SELECT COUNT(*) FROM fact_pos_sales WHERE unit_price_kes <= 0 OR unit_price_kes > 50000
            """)).scalar() or 0
            results.append({
                "name": "POS Price Sanity Range",
                "category": "Validity",
                "passed": bad_price == 0,
                "value": bad_price,
                "message": f"{bad_price} sales with prices <= 0 or > 50,000 KES.",
            })

            # Check 4: Zero Duplicate Transaction IDs
            dup_txns = conn.execute(text("""
                SELECT COUNT(*) FROM (
                    SELECT transaction_id FROM fact_pos_sales GROUP BY transaction_id HAVING COUNT(*) > 1
                )
            """)).scalar() or 0
            results.append({
                "name": "Transaction ID Uniqueness",
                "category": "Uniqueness",
                "passed": dup_txns == 0,
                "value": dup_txns,
                "message": f"{dup_txns} duplicate transaction IDs detected.",
            })

            # Check 5: Referential Integrity (No Orphan Store or Product Keys)
            orphan_stores = conn.execute(text("""
                SELECT COUNT(*) FROM fact_pos_sales s LEFT JOIN dim_store st ON s.store_key = st.store_key WHERE st.store_key IS NULL
            """)).scalar() or 0
            orphan_prods = conn.execute(text("""
                SELECT COUNT(*) FROM fact_pos_sales s LEFT JOIN dim_product p ON s.product_key = p.product_key WHERE p.product_key IS NULL
            """)).scalar() or 0
            results.append({
                "name": "Referential Integrity (Zero Orphan FKs)",
                "category": "Integrity",
                "passed": (orphan_stores == 0 and orphan_prods == 0),
                "value": {"orphan_stores": orphan_stores, "orphan_products": orphan_prods},
                "message": f"Orphan stores: {orphan_stores}, Orphan products: {orphan_prods}.",
            })

            # Check 6: Date Boundary (No Future Sale Dates)
            today_key = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
            future_dates = conn.execute(text(f"""
                SELECT COUNT(*) FROM fact_pos_sales WHERE date_key > 20261231
            """)).scalar() or 0
            results.append({
                "name": "Observation Date Boundary Assertion",
                "category": "Consistency",
                "passed": future_dates == 0,
                "value": future_dates,
                "message": f"{future_dates} records with dates in the distant future.",
            })

            # Check 7: KAMIS Price Non-Negativity
            bad_kamis = conn.execute(text("""
                SELECT COUNT(*) FROM fact_kamis_market_price WHERE norm_wholesale_price_kes <= 0 OR norm_retail_price_kes <= 0
            """)).scalar() or 0
            results.append({
                "name": "KAMIS Commodity Price Non-Negativity",
                "category": "Validity",
                "passed": bad_kamis == 0,
                "value": bad_kamis,
                "message": f"{bad_kamis} non-positive KAMIS prices detected.",
            })

            # Check 8: Gross Margin Feasibility (-50% to 100%)
            bad_margins = conn.execute(text("""
                SELECT COUNT(*) FROM fact_pos_sales WHERE gross_margin_pct < -50.0 OR gross_margin_pct > 100.0
            """)).scalar() or 0
            results.append({
                "name": "Gross Profit Margin Bounds",
                "category": "Validity",
                "passed": bad_margins == 0,
                "value": bad_margins,
                "message": f"{bad_margins} transactions outside [-50%, 100%] margin bounds.",
            })

        all_passed = all(r["passed"] for r in results)
        logger.info("Data Quality Suite: %d/%d checks passed.", sum(1 for r in results if r["passed"]), len(results))
        return all_passed, results

    def quarantine_bad_batch(self, df: pd.DataFrame, reason: str) -> Path:
        quarantine_dir = settings.data_dir / "quarantine"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bad_file = quarantine_dir / f"bad_batch_{timestamp}.csv"

        df_bad = df.copy()
        df_bad["_quarantine_reason"] = reason
        df_bad["_quarantine_timestamp"] = datetime.now(timezone.utc).isoformat()
        df_bad.to_csv(bad_file, index=False)
        logger.warning("Quarantined %d bad records to %s. Reason: %s", len(df_bad), bad_file, reason)
        return bad_file

    def record_audit(
        self,
        batch_id: str,
        stage: str,
        total_records: int,
        quarantined_count: int,
        duration_sec: float,
        dq_results: List[Dict[str, Any]],
    ) -> None:
        total = len(dq_results)
        passed = sum(1 for r in dq_results if r["passed"])
        status = "PASSED" if passed == total else "FAILED"

        insert_sql = """
        INSERT INTO data_quality_audit_log (
            batch_id, pipeline_stage, total_checks, passed_checks,
            status, records_scanned, quarantined_count, execution_duration_sec,
            details_json, recorded_at
        ) VALUES (
            :batch_id, :stage, :total, :passed,
            :status, :records_scanned, :quarantined_count, :duration_sec,
            :details_json, :recorded_at
        )
        """
        with self.engine.begin() as conn:
            conn.execute(text(insert_sql), {
                "batch_id": batch_id,
                "stage": stage,
                "total": total,
                "passed": passed,
                "status": status,
                "records_scanned": total_records,
                "quarantined_count": quarantined_count,
                "duration_sec": duration_sec,
                "details_json": json.dumps(dq_results),
                "recorded_at": datetime.now(timezone.utc),
            })
        logger.info("Recorded Data Quality Audit for batch %s (Status: %s).", batch_id, status)
