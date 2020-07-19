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


class KafkaAdmin:
    def __init__(self, kafka_config):
        self.admin = KafkaAdminClient(
                bootstrap_servers=kafka_config["bootstrap_servers"],
                security_protocol="SSL",
                ssl_cafile=kafka_config["ssl_cafile"],
                ssl_certfile=kafka_config["ssl_certfile"],
                ssl_keyfile=kafka_config["ssl_keyfile"])
        self.topic = kafka_config["topic"]
        self.num_partitions = kafka_config["num_partitions"]
        self.replication_factor = kafka_config["replication_factor"]
        self._create_topic()

    def _create_topic(self):
        new_topic = NewTopic(
                self.topic,
                self.num_partitions,
                self.replication_factor)
        try:
            logger.info("Ensuring topics %s", self.topic)
            self.admin.create_topics([new_topic])

            logger.info("Topic %s created", self.topic)
        except TopicAlreadyExistsError:
            logger.info("Topic %s already exists", self.topic)
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

    def __call__(self, check_result: CheckResult):
        message = json.dumps(check_result._asdict()).encode("utf-8")
        self.producer.send(self.topic, message)


class KafkaReader:
    def __init__(self, kafka_config, sink):
        self.consumer = KafkaConsumer(
                kafka_config["topic"],
                consumer_timeout_ms=1000,
                auto_offset_reset='earliest',
                bootstrap_servers=kafka_config["bootstrap_servers"],
                security_protocol="SSL",
                ssl_cafile=kafka_config["ssl_cafile"],
                ssl_certfile=kafka_config["ssl_certfile"],
                ssl_keyfile=kafka_config["ssl_keyfile"])
        self.sink = sink

    def run(self):
        for message in self.consumer:
            deserialized = CheckResult(**json.loads(message))
            self.sink(deserialized)
