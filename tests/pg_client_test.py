from context import web_monitor
from web_monitor.pg_client import PgClient

CONNECTION_STRING="postgres://avnadmin:hufb2mg21hm5aso4@pg-26b4b6c0-golovasteek-50e3.aivencloud.com:20595/defaultdb?sslmode=require"

def test_connect():
    client = PgClient(CONNECTION_STRING, "test_db", "status_log")
    client({
        "timestamp": "2020-07-14 10:00:00",
        "url": "http://example.com",
        "status_code": 200
    })

           
