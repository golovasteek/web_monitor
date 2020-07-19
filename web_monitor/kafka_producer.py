import json
import os
import logging

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

MAX_BLOCK_MS = 10000

SSL_CA_FILE = "./certs/ca.cert"
SSL_AUTH_CERT = "./certs/auth.cert"
SSL_KEY = "./certs/key"

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KafkaAdmin:
    def __init__(self, bootstrap_servers, topic):
        self.admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                security_protocol="SSL",
                ssl_cafile=SSL_CA_FILE,
                ssl_certfile=SSL_AUTH_CERT,
                ssl_keyfile=SSL_KEY)
        self.topic = topic
        self.num_partitions = 1  # FIXME: config
        self.replication_factor = 1  # FIXME: config
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

        
    def reset_content(self):
        deleted = False
        while not deleted:
            try: 
                logger.info("Deleting topic %s", self.topic)
                self.admin.delete_topics([self.topic])
            except UnknownTopicOrPartitionError:
                logger.info("Topic %s unknown, skipping", self.topic)
                deleted = True
        self._create_topic()


class KafkaSink:
    def __init__(self, bootstrap_servers, topic):
        assert os.path.isfile(SSL_CA_FILE)
        assert os.path.isfile(SSL_AUTH_CERT)
        assert os.path.isfile(SSL_KEY)

        self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks='all',  # TODO: make it async
                security_protocol="SSL",
                ssl_cafile=SSL_CA_FILE,
                ssl_certfile=SSL_AUTH_CERT,
                ssl_keyfile=SSL_KEY,
                max_block_ms=MAX_BLOCK_MS)
        self.topic = topic

    def __call__(self, test_report):
        message = json.dumps(test_report).encode("utf-8")
        self.producer.send(self.topic, message)

class KafkaReader:
    def __init__(self, bootstrap_servers, topic, sink):
        assert os.path.isfile(SSL_CA_FILE)
        assert os.path.isfile(SSL_AUTH_CERT)
        assert os.path.isfile(SSL_KEY)

        self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=1000,
                auto_offset_reset='earliest',
                security_protocol="SSL",
                ssl_cafile=SSL_CA_FILE,
                ssl_certfile=SSL_AUTH_CERT,
                ssl_keyfile=SSL_KEY)
        self.topic = topic
        self.sink = sink

    def run(self):
        for message in self.consumer:
            deserialized = json.loads(message)
            sink(message)
        
