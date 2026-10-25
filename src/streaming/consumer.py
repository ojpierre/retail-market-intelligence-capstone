import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import pandas as pd

from .kafka_config import (
    CONSUMER_GROUP_ANALYTICS,
    TOPIC_DEAD_LETTER,
    TOPIC_INVENTORY_MOVEMENTS,
    TOPIC_POS_TRANSACTIONS,
    TOPIC_PRICE_ALERTS,
)
from .producer import LocalMessageBroker

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


class RetailEventConsumer:
    def __init__(self, group_id: str = CONSUMER_GROUP_ANALYTICS):
        self.group_id = group_id
        self.broker = LocalMessageBroker()
        self.processed_records: List[Dict[str, Any]] = []
        self.dlq_records: List[Dict[str, Any]] = []
        logger.info("RetailEventConsumer subscribed as group: %s", group_id)

    def validate_payload(self, topic: str, payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False

        if topic == TOPIC_POS_TRANSACTIONS:
            required = ["transaction_id", "store_id", "product_id", "quantity", "unit_price_kes"]
            return all(k in payload and payload[k] is not None for k in required)
        elif topic == TOPIC_INVENTORY_MOVEMENTS:
            required = ["movement_id", "store_id", "product_id", "movement_type", "stock_on_hand"]
            return all(k in payload and payload[k] is not None for k in required)
        elif topic == TOPIC_PRICE_ALERTS:
            required = ["alert_id", "alert_type", "severity"]
            return all(k in payload for k in required)
        return True

    def poll_and_process(
        self,
        topic: str,
        max_records: int = 500,
        handler: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        raw_events = self.broker.consume(topic, self.group_id, max_records=max_records)
        valid_count = 0
        dlq_count = 0

        for record in raw_events:
            val = record.get("value", {})
            if self.validate_payload(topic, val):
                self.processed_records.append(record)
                valid_count += 1
                if handler:
                    handler(val)
            else:
                self.dlq_records.append(record)
                self.broker.publish(TOPIC_DEAD_LETTER, key=record.get("key", "dlq"), value=val)
                dlq_count += 1

        return {
            "group_id": self.group_id,
            "topic": topic,
            "polled_count": len(raw_events),
            "valid_processed": valid_count,
            "dlq_routed": dlq_count,
        }

    def persist_consumed_to_bronze(self, output_dir: Optional[Path] = None) -> Path:
        target_dir = output_dir or (settings.bronze_dir / "streaming")
        target_dir.mkdir(parents=True, exist_ok=True)
        sink_path = target_dir / f"stream_{self.group_id}_consumed.parquet"

        if not self.processed_records:
            logger.info("No consumed records to persist.")
            return sink_path

        flat_rows = []
        for r in self.processed_records:
            val = r["value"].copy()
            val["_kafka_topic"] = r["topic"]
            val["_kafka_partition"] = r["partition"]
            val["_kafka_offset"] = r["offset"]
            val["_kafka_ingested_at"] = r["timestamp"]
            flat_rows.append(val)

        df = pd.DataFrame(flat_rows)
        df.to_parquet(sink_path, index=False)
        logger.info("Persisted %d streaming records to Bronze: %s", len(df), sink_path)
        return sink_path
