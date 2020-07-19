from context import web_monitor
from web_monitor.kafka_producer import KafkaSink, KafkaReader, KafkaAdmin

from web_monitor.check_result import CheckResult

TEST_SERVERS = [
    "kafka-1548488a-golovasteek-50e3.aivencloud.com:20597"
]

TEST_TOPIC = "test_web_checker"
TEST_ITEM = CheckResult(
        timestamp=1,
        url="http://example.com",
        status_code=200
    )


def test_produce_consume():
    admin = KafkaAdmin(TEST_SERVERS, TEST_TOPIC)
    
    def mock_sink(report):
        pass

    consumer = KafkaReader(TEST_SERVERS, TEST_TOPIC, mock_sink)
    for message in consumer.consumer:
        pass

    producer = KafkaSink(TEST_SERVERS, TEST_TOPIC)
    producer(TEST_ITEM)
    producer.producer.close()

    messages = []
    for message in consumer.consumer:
        messages.append(message)

    assert len(messages) == 1
