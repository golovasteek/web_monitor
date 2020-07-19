from context import web_monitor  # noqa
from web_monitor.kafka_client import KafkaSink, KafkaReader

from web_monitor.check_result import CheckResult


TEST_CONFIG = {
    "bootstrap_servers": [
        "kafka-1548488a-golovasteek-50e3.aivencloud.com:20597"
    ],
    "ssl_cafile": "./certs/ca.cert",
    "ssl_certfile": "./certs/auth.cert",
    "ssl_keyfile": "./certs/key",
    "topic": "test_web_monitor",
    "num_partitions": 1,
    "replication_factor": 3,
    "consumer_group_id": "test_consumer_group"
}

TEST_ITEM = CheckResult(
        timestamp=1,
        url="http://example.com",
        status_code=200,
        response_time=0.1,
        match_content=None
    )


def test_produce_consume():
    messages = []

    def mock_sink(message):
        messages.append(message)

    with KafkaReader(TEST_CONFIG, mock_sink) as consumer:
        consumer.run()
        messages.clear()

        producer = KafkaSink(TEST_CONFIG)
        producer([TEST_ITEM])
        producer.producer.close()

        consumer.run()

        assert len(messages) == 1
