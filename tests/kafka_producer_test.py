from context import web_monitor  # noqa
from web_monitor.kafka_producer import KafkaSink, KafkaReader, KafkaAdmin

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
    "replication_factor": 3
}

TEST_ITEM = CheckResult(
        timestamp=1,
        url="http://example.com",
        status_code=200
    )


def test_produce_consume():
    admin = KafkaAdmin(TEST_CONFIG)

    def mock_sink(report):
        pass

    consumer = KafkaReader(TEST_CONFIG, mock_sink)
    for message in consumer.consumer:
        pass

    producer = KafkaSink(TEST_CONFIG)
    producer(TEST_ITEM)
    producer.producer.close()

    messages = []
    for message in consumer.consumer:
        messages.append(message)

    assert len(messages) == 1
