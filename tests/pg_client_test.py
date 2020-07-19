import os
from context import web_monitor  # noqa
from web_monitor.pg_client import PgClient
from web_monitor.check_result import CheckResult


TEST_ITEM = CheckResult(
    timestamp=1,
    url="http://example.com",
    status_code=200
    )

TEST_PG_CONFIG = {
    "dbname": "test_web_monitor",
    "default_dbname": "defaultdb",
    "host": "pg-26b4b6c0-golovasteek-50e3.aivencloud.com",
    "port": 20595,
    "user": "avnadmin",
    "pw_file": "./certs/pg_password"
}


def test_connect():
    with PgClient(TEST_PG_CONFIG) as client:
        client(TEST_ITEM)
