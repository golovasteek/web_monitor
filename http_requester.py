import requests

#TODO: make an enum
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
            try:
                resp = requests.get(url, timeout=1.0)
                self.sink({
                    "url": url,
                    "status_code": resp.status_code
                    })
            except requests.exceptions.ConnectionError:
                self.sink({
                    "url": url,
                    "status_code": CONNECTION_REFUSED
                })



