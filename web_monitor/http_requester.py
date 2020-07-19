import time
import logging

import requests

from web_monitor.check_result import CheckResult

# TODO: make an enum
CONNECTION_REFUSED = 521

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def do_requests(pages_configuration, sink):
    """
    """
    logger.info("Performing requests")
    for page in pages_configuration:
        url = page["url"]
        logger.debug("Checking %s", url)
        timestamp = int(time.time())
        try:
            # FIXME: make timeout configurable
            resp = requests.get(url, timeout=5.0)
            result = CheckResult(
                timestamp=timestamp,
                url=url,
                status_code=resp.status_code)

            sink(result)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            result = CheckResult(
                timestamp=timestamp,
                url=url,
                status_code=CONNECTION_REFUSED)
            sink(result)
