#!/usr/bin/env python3
from argparse import ArgumentParser
import time

import yaml
from web_monitor.http_requester import HttpRequester
from web_monitor.kafka_producer import KafkaSink

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", help="YAML config file", required=True)

    args = parser.parse_args()
    with open(args.config, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.SafeLoader)

    sink = KafkaSink(config["kafka"])
    requester = HttpRequester(config, sink)

    try:
        while True:
            requester.do_requests()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
