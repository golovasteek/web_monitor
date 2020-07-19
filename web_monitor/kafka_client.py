import json
import logging

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from web_monitor.check_result import CheckResult

MAX_BLOCK_MS = 10000


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _ensure_topics(kafka_config):
    admin = KafkaAdminClient(
       bootstrap_servers=kafka_config["bootstrap_servers"],
       security_protocol="SSL",
       ssl_cafile=kafka_config["ssl_cafile"],
       ssl_certfile=kafka_config["ssl_certfile"],
       ssl_keyfile=kafka_config["ssl_keyfile"])
    topic = kafka_config["topic"]
    num_partitions = kafka_config["num_partitions"]
    replication_factor = kafka_config["replication_factor"]

    new_topic = NewTopic(
            topic,
            num_partitions,
            replication_factor)
    try:
        logger.info("Ensuring topics %s", topic)
        admin.create_topics([new_topic])

        logger.info("Topic %s created", topic)
    except TopicAlreadyExistsError:
        logger.info("Topic %s already exists", topic)
        return True


class KafkaSink:
    def __init__(self, kafka_config):
        self.producer = KafkaProducer(
                bootstrap_servers=kafka_config["bootstrap_servers"],
                security_protocol="SSL",
                ssl_cafile=kafka_config["ssl_cafile"],
                ssl_certfile=kafka_config["ssl_certfile"],
                ssl_keyfile=kafka_config["ssl_keyfile"],
                max_block_ms=MAX_BLOCK_MS)
        self.topic = kafka_config["topic"]
        _ensure_topics(kafka_config)

    def __call__(self, check_results):
        assert len(check_results) == 1, "Only collections of size 1 are supported by KafkaSink"
        message = json.dumps(check_results[0]._asdict()).encode("utf-8")
        self.producer.send(self.topic, message)


class KafkaReader:
    def __init__(self, kafka_config, sink):
        self.config = kafka_config
        self.sink = sink

    def __enter__(self):
        self.consumer = KafkaConsumer(
                self.config["topic"],
                auto_offset_reset='earliest',
                group_id=self.config["consumer_group_id"],
                bootstrap_servers=self.config["bootstrap_servers"],
                value_deserializer=lambda x: CheckResult(**json.loads(x)),
                security_protocol="SSL",
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"])
        self.messages_read = 0
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.consumer.close(autocommit=False)

    def run(self):
        poll_result = self.consumer.poll(update_offsets=False)
        messages = poll_result.get(self.config["topic"], [])

        self.sink([message.value for message in messages])
        self.messages_read += len(messages)
        if self.messages_read // 100 > (self.messages_read - len(messages)) // 100:
            logger.info(
                "Messages read: %d, recent offset: %d",
                self.messages_read, messages[-1].offset)

        self.consumer.commit()