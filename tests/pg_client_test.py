import os
from context import web_monitor
from web_monitor.pg_client import PgClient
from web_monitor.check_result import CheckResult

CONNECTION_STRING = os.environ["TEST_PG_CONNECTION_STRING"]

TEST_ITEM=CheckResult(
    timestamp=1,
    url="http://example.com",
    status_code=200
    )

def test_connect():
    client = PgClient(CONNECTION_STRING, "test_db", "status_log")
    client(TEST_ITEM)

           
