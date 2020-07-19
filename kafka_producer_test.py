from kafka_producer import KafkaSink, KafkaReader, KafkaAdmin

TEST_SERVERS = [
    "kafka-1548488a-golovasteek-50e3.aivencloud.com:20597"
]

TEST_TOPIC = "test_web_checker"


def test_simple():
    admin = KafkaAdmin(TEST_SERVERS, TEST_TOPIC)

    sink = KafkaSink(TEST_SERVERS, TEST_TOPIC)

    sink({
        "url": "http://example.com",
        "status_code": 200
    })


def test_produce_consume():
    admin = KafkaAdmin(TEST_SERVERS, TEST_TOPIC)
    
    consumer = KafkaReader(TEST_SERVERS, TEST_TOPIC)
    for message in consumer.consumer:
        pass

    producer = KafkaSink(TEST_SERVERS, TEST_TOPIC)
    producer(
            {
                "url": "http://example.com",
                "status_code": 200
            })
    producer.producer.close()

    messages = []
    for message in consumer.consumer:
        messages.append(message)

    assert len(messages) == 1
