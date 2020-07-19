import requests
from web_monitor.check_result import CheckResult
import time

# TODO: make an enum
CONNECTION_REFUSED = 521


class HttpRequester:
    def __init__(self, configuration: dict, sink):
        self.config = configuration
        self.sink = sink

    def __start__(self):
        pass

    def do_requests(self):
        for page in self.config["pages"]:
            url = page["url"]
            timestamp = int(time.time())
            try:
                resp = requests.get(url, timeout=1.0)
                result = CheckResult(
                    timestamp=timestamp,
                    url=url,
                    status_code=resp.status_code)

                self.sink(result)
            except requests.exceptions.ConnectionError:
                result = CheckResult(
                    timestamp=timestamp,
                    url=url,
                    status_code=CONNECTION_REFUSED)
                self.sink(result)
