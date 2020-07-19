import psycopg2

from context import web_monitor  # noqa
from web_monitor.pg_client import PgClient
from web_monitor.check_result import CheckResult


TEST_ITEM = CheckResult(
    timestamp=1,
    url="http://example.com",
    status_code=200,
    response_time=0.1,
    match_content=None
    )

TEST_PG_CONFIG = {
    "dbname": "test_web_monitor",
    "default_dbname": "defaultdb",
    "host": "pg-26b4b6c0-golovasteek-50e3.aivencloud.com",
    "port": 20595,
    "user": "avnadmin",
    "pw_file": "./certs/pg_password"
}

PASSWORD = open(TEST_PG_CONFIG["pw_file"], "r").read()


def test_connect_with_empty_db():

    with psycopg2.connect(
            host=TEST_PG_CONFIG["host"],
            port=TEST_PG_CONFIG["port"],
            user=TEST_PG_CONFIG["user"],
            dbname=TEST_PG_CONFIG["default_dbname"],
            password=PASSWORD,
            sslmode='require') as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("DROP DATABASE {};".format(TEST_PG_CONFIG["dbname"]))

    with PgClient(TEST_PG_CONFIG) as client:
        client([TEST_ITEM])

    with psycopg2.connect(
            host=TEST_PG_CONFIG["host"],
            port=TEST_PG_CONFIG["port"],
            user=TEST_PG_CONFIG["user"],
            dbname=TEST_PG_CONFIG["dbname"],
            password=PASSWORD,
            sslmode='require') as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM {};".format(client.table))
            result = cursor.fetchall()
            print(result)
            assert result[0][0] == 1
