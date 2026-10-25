import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd

from .kafka_config import (
    CONSUMER_GROUP_ANALYTICS,
    CONSUMER_GROUP_INVENTORY,
    TOPIC_INVENTORY_MOVEMENTS,
    TOPIC_POS_TRANSACTIONS,
    TOPIC_PRICE_ALERTS,
)
from .producer import RetailEventProducer, LocalMessageBroker
from .consumer import RetailEventConsumer

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


class StreamEventProcessor:
    def __init__(self, group_prefix: str = "retail-stream"):
        self.broker = LocalMessageBroker()
        self.producer = RetailEventProducer()
        unique_id = f"{group_prefix}-{int(time.time() * 1000)}"
        self.pos_consumer = RetailEventConsumer(group_id=f"{unique_id}-pos")
        self.inv_consumer = RetailEventConsumer(group_id=f"{unique_id}-inv")
        self.alerts_generated: List[Dict[str, Any]] = []

    def handle_inventory_event(self, event: Dict[str, Any]) -> None:
        stock = event.get("stock_on_hand", 0)
        reorder = event.get("reorder_point", 35)

        if stock < reorder:
            alert = {
                "alert_id": f"ALT-STK-{event.get('store_id')}-{event.get('product_id')}",
                "alert_type": "STOCKOUT_WARNING",
                "severity": "CRITICAL" if stock <= 10 else "HIGH",
                "store_id": event.get("store_id"),
                "product_id": event.get("product_id"),
                "current_stock": stock,
                "reorder_threshold": reorder,
                "message": f"Inventory at {event.get('store_id')} for {event.get('product_id')} dropped to {stock} (threshold: {reorder})",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.alerts_generated.append(alert)
            self.producer.produce_price_alert(alert)

    def run_streaming_simulation(self, sample_size: int = 500) -> Dict[str, Any]:
        logger.info("Starting live streaming event simulation (Batch size: %d)...", sample_size)

        pos_master = settings.raw_operational_dir / "pos_transactions_master.csv"
        inv_master = settings.raw_operational_dir / "inventory_movements_master.csv"

        if not pos_master.exists() or not inv_master.exists():
            from ..ingestion.operational_generator import ingest_operational_data
            ingest_operational_data()

        df_pos = pd.read_csv(pos_master).head(sample_size)
        df_inv = pd.read_csv(inv_master).head(sample_size)

        pos_records = df_pos.to_dict(orient="records")
        inv_records = df_inv.to_dict(orient="records")

        # Record starting offsets
        start_pos_offset = len(self.broker.topics[TOPIC_POS_TRANSACTIONS])
        start_inv_offset = len(self.broker.topics[TOPIC_INVENTORY_MOVEMENTS])
        self.broker.consumer_offsets[self.pos_consumer.group_id][TOPIC_POS_TRANSACTIONS] = start_pos_offset
        self.broker.consumer_offsets[self.inv_consumer.group_id][TOPIC_INVENTORY_MOVEMENTS] = start_inv_offset

        pub_pos = self.producer.produce_batch(TOPIC_POS_TRANSACTIONS, pos_records, key_field="store_id")
        pub_inv = self.producer.produce_batch(TOPIC_INVENTORY_MOVEMENTS, inv_records, key_field="product_id")

        pos_res = self.pos_consumer.poll_and_process(TOPIC_POS_TRANSACTIONS, max_records=sample_size)
        inv_res = self.inv_consumer.poll_and_process(
            TOPIC_INVENTORY_MOVEMENTS,
            max_records=sample_size,
            handler=self.handle_inventory_event,
        )

        bronze_pos_sink = self.pos_consumer.persist_consumed_to_bronze()

        summary = {
            "published_pos_events": pub_pos,
            "published_inventory_events": pub_inv,
            "consumed_pos": pos_res,
            "consumed_inv": inv_res,
            "alerts_generated": len(self.alerts_generated),
            "bronze_sink_path": str(bronze_pos_sink),
        }
        logger.info("Kafka streaming simulation complete: %d alerts triggered, events persisted to Bronze.", len(self.alerts_generated))
        return summary
