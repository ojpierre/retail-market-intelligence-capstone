"""Streaming module for Kafka producers, consumers, and event processing."""
from .kafka_config import (
    TOPIC_POS_TRANSACTIONS,
    TOPIC_INVENTORY_MOVEMENTS,
    TOPIC_PRICE_ALERTS,
    TOPIC_DEAD_LETTER,
    CONSUMER_GROUP_ANALYTICS,
    CONSUMER_GROUP_INVENTORY,
)
from .producer import RetailEventProducer, LocalMessageBroker
from .consumer import RetailEventConsumer
from .event_processor import StreamEventProcessor
