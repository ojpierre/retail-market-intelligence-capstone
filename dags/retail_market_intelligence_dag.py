from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys

# Ensure repository root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.exceptions import AirflowException
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

logger = logging.getLogger("airflow.task")


def task_ingest_kamis(**context):
    from src.ingestion.kamis import ingest_kamis_data
    path, meta = ingest_kamis_data()
    logger.info("KAMIS Ingested: %d records", meta["rows"])
    return meta


def task_ingest_knbs(**context):
    from src.ingestion.knbs import ingest_knbs_data
    path, meta = ingest_knbs_data()
    logger.info("KNBS Ingested: %d records", meta["rows"])
    return meta


def task_ingest_operational(**context):
    from src.ingestion.operational_generator import ingest_operational_data
    path, meta = ingest_operational_data()
    logger.info("Operational Ingested: %d Txns, %d Inv", meta["pos_count"], meta["inventory_count"])
    return meta


def task_build_bronze(**context):
    from src.processing.bronze import build_bronze_layer
    summary = build_bronze_layer()
    logger.info("Bronze Layer Built: %s", summary)
    return summary


def task_build_silver(**context):
    from src.processing.silver import build_silver_layer
    summary = build_silver_layer()
    logger.info("Silver Layer Built: %s", summary)
    return summary


def task_build_gold(**context):
    from src.processing.gold import build_gold_layer
    summary = build_gold_layer()
    logger.info("Gold Layer Built: %s", summary)
    return summary


def task_load_warehouse(**context):
    from src.warehouse import get_engine, init_warehouse_schema, populate_dimensions, load_facts
    engine = get_engine()
    init_warehouse_schema(engine)
    dims = populate_dimensions(engine)
    facts = load_facts(engine, mode="replace")
    logger.info("Warehouse populated: %s dims, %s facts", dims, facts)
    return facts


def task_data_quality_gate(**context):
    from src.quality.engine import DataQualityEngine
    from src.warehouse import get_engine
    engine = get_engine()
    dq = DataQualityEngine(engine)
    all_passed, results = dq.run_all_assertions()
    if not all_passed:
        failed_checks = [r["name"] for r in results if not r["passed"]]
        raise Exception(f"Data Quality Gate Failed on: {failed_checks}")
    logger.info("Data Quality Gate Passed: %d/%d assertions verified.", len(results), len(results))
    return {"status": "PASSED", "passed_checks": len(results)}


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="kenya_retail_market_intelligence_pipeline",
        default_args=default_args,
        description="End-to-end batch & streaming data engineering pipeline for Kenya retail & KAMIS intelligence",
        schedule_interval="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["retail", "kamis", "knbs", "medallion", "capstone"],
    )

    t_ingest_kamis = PythonOperator(task_id="ingest_kamis", python_callable=task_ingest_kamis, dag=dag)
    t_ingest_knbs = PythonOperator(task_id="ingest_knbs", python_callable=task_ingest_knbs, dag=dag)
    t_ingest_operational = PythonOperator(task_id="ingest_operational", python_callable=task_ingest_operational, dag=dag)
    t_bronze = PythonOperator(task_id="spark_bronze_layer", python_callable=task_build_bronze, dag=dag)
    t_silver = PythonOperator(task_id="spark_silver_layer", python_callable=task_build_silver, dag=dag)
    t_gold = PythonOperator(task_id="spark_gold_layer", python_callable=task_build_gold, dag=dag)
    t_warehouse = PythonOperator(task_id="load_warehouse", python_callable=task_load_warehouse, dag=dag)
    t_quality_gate = PythonOperator(task_id="data_quality_gate", python_callable=task_data_quality_gate, dag=dag)

    [t_ingest_kamis, t_ingest_knbs, t_ingest_operational] >> t_bronze >> t_silver >> t_gold >> t_warehouse >> t_quality_gate
