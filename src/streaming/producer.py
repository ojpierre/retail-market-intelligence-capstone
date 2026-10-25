import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict

from .kafka_config import (
    TOPIC_POS_TRANSACTIONS,
    TOPIC_INVENTORY_MOVEMENTS,
    TOPIC_PRICE_ALERTS,
    TOPIC_DEAD_LETTER,
)

logger = logging.getLogger(__name__)


class LocalMessageBroker:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LocalMessageBroker, cls).__new__(cls)
            cls._instance.topics = defaultdict(list)
            cls._instance.consumer_offsets = defaultdict(lambda: defaultdict(int))
        return cls._instance

    def publish(self, topic: str, key: str, value: Dict[str, Any]) -> Dict[str, Any]:
        offset = len(self.topics[topic])
        record = {
            "topic": topic,
            "partition": hash(key) % 4 if key else 0,
            "offset": offset,
            "key": key,
            "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.topics[topic].append(record)
        return record

    def consume(self, topic: str, group_id: str, max_records: int = 100) -> List[Dict[str, Any]]:
        current_offset = self.consumer_offsets[group_id][topic]
        available = self.topics[topic][current_offset: current_offset + max_records]
        self.consumer_offsets[group_id][topic] += len(available)
        return available

    def reset_offsets(self, group_id: Optional[str] = None):
        if group_id:
            self.consumer_offsets[group_id].clear()
        else:
            self.consumer_offsets.clear()
            self.topics.clear()


class RetailEventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.broker = LocalMessageBroker()
        self.total_published = 0
        self.total_errors = 0
        logger.info("RetailEventProducer initialized for bootstrap servers: %s", bootstrap_servers)

    def produce(self, topic: str, key: str, value: Dict[str, Any]) -> Dict[str, Any]:
        try:
            record = self.broker.publish(topic, key, value)
            self.total_published += 1
            return record
        except Exception as exc:
            self.total_errors += 1
            logger.error("Failed to produce event to topic %s: %s", topic, exc)
            raise

    def produce_pos_transaction(self, txn_payload: Dict[str, Any]) -> Dict[str, Any]:
        key = str(txn_payload.get("store_id", "default_store"))
        return self.produce(TOPIC_POS_TRANSACTIONS, key, txn_payload)

    def produce_inventory_movement(self, inv_payload: Dict[str, Any]) -> Dict[str, Any]:
        key = str(inv_payload.get("product_id", "default_product"))
        return self.produce(TOPIC_INVENTORY_MOVEMENTS, key, inv_payload)

    def produce_price_alert(self, alert_payload: Dict[str, Any]) -> Dict[str, Any]:
        key = str(alert_payload.get("alert_type", "general_alert"))
        return self.produce(TOPIC_PRICE_ALERTS, key, alert_payload)

    def produce_batch(self, topic: str, events: List[Dict[str, Any]], key_field: str) -> int:
        count = 0
        for ev in events:
            key = str(ev.get(key_field, ""))
            self.produce(topic, key, ev)
            count += 1
        return count
