from pathlib import Path
from src.streaming import (
    RetailEventProducer,
    RetailEventConsumer,
    StreamEventProcessor,
    TOPIC_POS_TRANSACTIONS,
    TOPIC_INVENTORY_MOVEMENTS,
    TOPIC_DEAD_LETTER,
    CONSUMER_GROUP_ANALYTICS,
)


def test_producer_and_consumer_flow(sample_pos_record):
    producer = RetailEventProducer()
    consumer = RetailEventConsumer(group_id="test-consumer-group")

    record = producer.produce_pos_transaction(sample_pos_record)
    assert record["topic"] == TOPIC_POS_TRANSACTIONS
    assert record["key"] == "STR-001"

    res = consumer.poll_and_process(TOPIC_POS_TRANSACTIONS, max_records=10)
    assert res["valid_processed"] >= 1
    assert res["dlq_routed"] == 0


def test_dead_letter_routing():
    producer = RetailEventProducer()
    consumer = RetailEventConsumer(group_id="test-dlq-group")

    invalid_payload = {"some_random_field": 123}
    producer.produce(TOPIC_POS_TRANSACTIONS, key="invalid_key", value=invalid_payload)

    res = consumer.poll_and_process(TOPIC_POS_TRANSACTIONS, max_records=10)
    assert res["dlq_routed"] >= 1


def test_stream_event_processor():
    processor = StreamEventProcessor()
    summary = processor.run_streaming_simulation(sample_size=50)
    assert summary["published_pos_events"] == 50
    assert summary["published_inventory_events"] == 50
    assert summary["consumed_pos"]["valid_processed"] == 50
