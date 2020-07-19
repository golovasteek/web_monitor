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
        self.config = kafka_config
        self.messages_sent = 0
        _ensure_topics(kafka_config)

    def __enter__(self):
        self.producer = KafkaProducer(
                bootstrap_servers=self.config["bootstrap_servers"],
                security_protocol="SSL",
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
                max_block_ms=MAX_BLOCK_MS)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("Flushing pending records")
        self.producer.flush()
        logger.info("Exited Kafka Sink. %d messages sent", self.messages_sent)
        self.producer.close()

    def __call__(self, check_results):
        assert len(check_results) == 1, "Only collections of size 1 are supported by KafkaSink"
        message = json.dumps(check_results[0]._asdict()).encode("utf-8")
        self.producer.send(self.config["topic"], message).add_callback(
            lambda m: logger.debug("Message sent %s %d %d", m.topic, m.partition, m.offset)).add_errback(
            lambda exc: logger.exception("Send failed", exc))
        self.messages_sent += 1


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
                enable_auto_commit=False,
                security_protocol="SSL",
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"])
        logger.debug(
            self.consumer.partitions_for_topic(self.config["topic"]))
        self.messages_read = 0
        self.last_offset = -1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._log_status()
        logger.info("Exiting consumer...")
        self.consumer.close(autocommit=False)

    def _log_status(self):
        logger.info(
            "Messages read: %d, recent offset: %d",
            self.messages_read, self.last_offset)

    def run(self):
        poll_result = self.consumer.poll(timeout_ms=100)
        messages = []
        for topic_parition, partition_messages in poll_result.items():
            assert topic_parition.topic == self.config["topic"]
            messages += partition_messages

        if not messages:
            return

        # NOTE: If message processing takes more than max_poll_timeout_ms
        # which defaults to 300000 (5 min), our consumer can be considered dead
        # and will stop receving messages.
        self.sink(message.value for message in messages)
        self.messages_read += len(messages)
        self.last_offset = messages[-1].offset
        if self.messages_read // 100 > (self.messages_read - len(messages)) // 100:
            self._log_status()

        # commit offsets, only if sink call was successful
        self.consumer.commit()
