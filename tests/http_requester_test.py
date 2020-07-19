from context import web_monitor  # noqa
from web_monitor.http_requester import do_requests
from http.server import HTTPServer, BaseHTTPRequestHandler
import random
import threading
import pytest

random.seed(1987)


class MockRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write("OK".encode("utf-8"))


@pytest.fixture
def http_server():
    # We do not search for the free port upfront, since we can not ensure
    # that port will not be acquired by other process after we selected it
    # TODO: handle the case, when server can not be create due-to port number conflict
    port = random.randrange(32000, 64000)
    server = HTTPServer(('localhost', port), MockRequestHandler)

    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server, thread


class SinkMock():
    def __init__(self):
        self.reports = []

    def __call__(self, test_report):
        self.reports.append(test_report)


def test_non_existing_url():
    configuration = [
            {
                "url": "http://localhost:8080"
            }
        ]
    sink = SinkMock()

    do_requests(configuration, sink)

    assert len(sink.reports) == 1
    assert sink.reports[0].status_code == 521


def test_success(http_server):
    server, thread = http_server
    configuration = [
            {
                "url": "http://localhost:{port}".format(port=server.server_port)
            }
        ]
    sink = SinkMock()
    do_requests(configuration, sink)

    assert len(sink.reports) == 1
    assert sink.reports[0].status_code == 200

# TODO:
# test for connection timeout
