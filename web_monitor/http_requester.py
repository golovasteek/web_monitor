import time
import logging

import requests

from web_monitor.check_result import CheckResult

# TODO: make an enum
CONNECTION_REFUSED = 521

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class HttpRequester:
    def __init__(self, configuration: dict, sink):
        self.config = configuration
        self.sink = sink

    def __start__(self):
        pass

    def do_requests(self):
        logger.info("Performing requests")
        for page in self.config["pages"]:
            url = page["url"]
            logger.debug("Checking %s", url)
            timestamp = int(time.time())
            try:
                resp = requests.get(url, timeout=5.0)
                result = CheckResult(
                    timestamp=timestamp,
                    url=url,
                    status_code=resp.status_code)

                self.sink(result)
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
                result = CheckResult(
                    timestamp=timestamp,
                    url=url,
                    status_code=CONNECTION_REFUSED)
                self.sink(result)
