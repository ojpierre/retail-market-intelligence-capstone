from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class KafkaTopicConfig:
    name: str
    num_partitions: int
    replication_factor: int
    cleanup_policy: str
    retention_ms: int
    description: str


TOPIC_POS_TRANSACTIONS = "retail.pos.transactions"
TOPIC_INVENTORY_MOVEMENTS = "retail.inventory.movements"
TOPIC_PRICE_ALERTS = "market.price.alerts"
TOPIC_DEAD_LETTER = "retail.dlq.errors"

CONSUMER_GROUP_ANALYTICS = "retail-analytics-group"
CONSUMER_GROUP_INVENTORY = "retail-inventory-monitor"
CONSUMER_GROUP_GOVERNANCE = "retail-governance-audit"

TOPIC_CONFIGS: Dict[str, KafkaTopicConfig] = {
    TOPIC_POS_TRANSACTIONS: KafkaTopicConfig(
        name=TOPIC_POS_TRANSACTIONS,
        num_partitions=6,
        replication_factor=3,
        cleanup_policy="delete",
        retention_ms=604800000,  # 7 days
        description="Stream of point-of-sale customer sales transactions keyed by store_id",
    ),
    TOPIC_INVENTORY_MOVEMENTS: KafkaTopicConfig(
        name=TOPIC_INVENTORY_MOVEMENTS,
        num_partitions=4,
        replication_factor=3,
        cleanup_policy="delete",
        retention_ms=604800000,
        description="Stream of stock-on-hand restocks, shrinkage and sales dispatch keyed by product_id",
    ),
    TOPIC_PRICE_ALERTS: KafkaTopicConfig(
        name=TOPIC_PRICE_ALERTS,
        num_partitions=3,
        replication_factor=3,
        cleanup_policy="delete",
        retention_ms=2592000000,  # 30 days
        description="Real-time alerts triggered on significant retail vs KAMIS price divergences and stockouts",
    ),
    TOPIC_DEAD_LETTER: KafkaTopicConfig(
        name=TOPIC_DEAD_LETTER,
        num_partitions=2,
        replication_factor=3,
        cleanup_policy="compact",
        retention_ms=2592000000,
        description="Dead-letter queue capturing corrupted or schema-violating event payloads",
    ),
}
