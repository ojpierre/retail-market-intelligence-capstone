import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from ingestion.pipeline import run_batch_ingestion
from streaming import StreamEventProcessor
from processing import build_bronze_layer, build_silver_layer, build_gold_layer
from warehouse import get_engine, init_warehouse_schema, populate_dimensions, load_facts
from quality import DataQualityEngine

logger = logging.getLogger("retail_market_intelligence")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_pipeline(
    mode: str = "replace",
    database_url: Optional[str] = None,
    simulate_stream: bool = True,
    force_ingest: bool = False,
) -> Dict[str, Any]:
    start_time = time.time()
    batch_id = f"BATCH-RMI-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    logger.info("=================================================================")
    logger.info("Kenya National Retail and Market Intelligence Platform")
    logger.info("Batch ID: %s | Mode: %s | Stream Simulation: %s", batch_id, mode, simulate_stream)
    logger.info("=================================================================")

    # Stage 1: Batch Ingestion (Milestone 1)
    logger.info("[Stage 1/5] Ingesting KAMIS, KNBS, and Operational Datasets...")
    ingest_summary = run_batch_ingestion(force=force_ingest)

    # Stage 2: Kafka Streaming Simulation (Milestone 2)
    streaming_summary = {}
    if simulate_stream:
        logger.info("[Stage 2/5] Running Kafka Event Streaming & Consumer Groups...")
        stream_proc = StreamEventProcessor()
        streaming_summary = stream_proc.run_streaming_simulation(sample_size=500)

    # Stage 3: Spark Medallion Transformations (Milestone 3)
    logger.info("[Stage 3/5] Executing Medallion Transformations (Bronze -> Silver -> Gold)...")
    bronze_res = build_bronze_layer()
    silver_res = build_silver_layer()
    gold_res = build_gold_layer()

    # Stage 4: Star Schema Dimensional Warehouse (Milestone 4)
    logger.info("[Stage 4/5] Populating Warehouse Star Schema and Loading Facts...")
    engine = get_engine(database_url)
    init_warehouse_schema(engine)
    dims_res = populate_dimensions(engine)
    facts_res = load_facts(engine, mode=mode)

    # Stage 5: Data Quality Gate and Governance (Milestone 6)
    logger.info("[Stage 5/5] Executing Data Quality Suite & Recording Lineage...")
    dq = DataQualityEngine(engine)
    all_passed, dq_results = dq.run_all_assertions()

    duration = round(time.time() - start_time, 2)
    dq.record_audit(
        batch_id=batch_id,
        stage="warehouse_gold",
        total_records=facts_res.get("fact_pos_sales", 0),
        quarantined_count=0,
        duration_sec=duration,
        dq_results=dq_results,
    )

    logger.info("=================================================================")
    logger.info("Platform Execution Completed in %.2f seconds", duration)
    logger.info(" - Ingestion: %d KAMIS rows, %d KNBS rows, %d POS Txns",
                ingest_summary["kamis_rows"], ingest_summary["knbs_rows"], ingest_summary["pos_transactions_count"])
    logger.info(" - Streaming: %d POS published, %d Alerts triggered",
                streaming_summary.get("published_pos_events", 0), streaming_summary.get("alerts_generated", 0))
    logger.info(" - Warehouse Facts: %d POS Sales, %d Inventory Logs, %d Price Intel rows",
                facts_res["fact_pos_sales"], facts_res["fact_inventory_snapshot"], facts_res["fact_price_intelligence"])
    logger.info(" - Data Quality Status: %s (%d/%d checks passed)",
                "PASSED" if all_passed else "FAILED",
                sum(1 for r in dq_results if r["passed"]), len(dq_results))
    logger.info("=================================================================")

    return {
        "batch_id": batch_id,
        "mode": mode,
        "duration_sec": duration,
        "ingest": ingest_summary,
        "streaming": streaming_summary,
        "bronze": bronze_res,
        "silver": silver_res,
        "gold": gold_res,
        "dims": dims_res,
        "facts": facts_res,
        "dq_status": "PASSED" if all_passed else "FAILED",
        "dq_passed_checks": sum(1 for r in dq_results if r["passed"]),
        "dq_total_checks": len(dq_results),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Kenya National Retail & Market Intelligence Platform - Master Pipeline"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["append", "replace"],
        default="replace",
        help="Warehouse loading mode: append or replace (default: replace).",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Database connection URL override (PostgreSQL or SQLite).",
    )
    parser.add_argument(
        "--no-stream",
        action="store_true",
        help="Disable Kafka streaming simulation stage.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-generation and overwrite of raw input datasets.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity (default: INFO).",
    )
    return parser


def main(args: Optional[List[str]] = None) -> int:
    parser = build_parser()
    parsed = parser.parse_args(args)
    setup_logging(parsed.log_level)

    try:
        res = run_pipeline(
            mode=parsed.mode,
            database_url=parsed.db_url,
            simulate_stream=not parsed.no_stream,
            force_ingest=parsed.force,
        )
        return 0 if res["dq_status"] == "PASSED" else 1
    except Exception as exc:
        logger.exception("Pipeline master execution failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
