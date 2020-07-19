#!/usr/bin/env python3
from argparse import ArgumentParser
import time

import yaml
from web_monitor.http_requester import do_requests
from web_monitor.kafka_producer import KafkaSink

DESCRIPTION = """
Periodically check the web-pages availability and publish test results into Kafka.
"""

if __name__ == "__main__":
    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument("-c", "--config", help="YAML config file", required=True)

    args = parser.parse_args()
    with open(args.config, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.SafeLoader)

    sink = KafkaSink(config["kafka"])
    
    try:
        while True:
            do_requests(config["pages"], sink)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
