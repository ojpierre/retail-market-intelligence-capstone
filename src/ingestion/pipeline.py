import logging
import time
from typing import Any, Dict
from .kamis import ingest_kamis_data
from .knbs import ingest_knbs_data
from .operational_generator import ingest_operational_data

logger = logging.getLogger(__name__)


def run_batch_ingestion(force: bool = False) -> Dict[str, Any]:
    start_time = time.time()
    logger.info("=================================================================")
    logger.info("Executing Milestone 1: Re-runnable Multi-Source Batch Ingestion")
    logger.info("=================================================================")

    kamis_path, kamis_meta = ingest_kamis_data(force=force)
    knbs_path, knbs_meta = ingest_knbs_data(force=force)
    op_path, op_meta = ingest_operational_data(force=force)

    duration = round(time.time() - start_time, 2)

    summary = {
        "status": "SUCCESS",
        "duration_sec": duration,
        "kamis_rows": kamis_meta["rows"],
        "kamis_sha256": kamis_meta["checksum_sha256"][:12],
        "knbs_rows": knbs_meta["rows"],
        "knbs_sha256": knbs_meta["checksum_sha256"][:12],
        "stores_count": op_meta["stores_count"],
        "products_count": op_meta["products_count"],
        "suppliers_count": op_meta["suppliers_count"],
        "pos_transactions_count": op_meta["pos_count"],
        "inventory_events_count": op_meta["inventory_count"],
    }

    logger.info("Milestone 1 Complete in %.2fs: Landed %d KAMIS rows, %d KNBS rows, %d POS Txns",
                duration, summary["kamis_rows"], summary["knbs_rows"], summary["pos_transactions_count"])
    return summary
