import logging
import psycopg2
from web_monitor.check_result import CheckResult

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PgClient():
    def __init__(self, pg_config):
        self.config = pg_config
        self.table = "check_result"

        with open(self.config["pw_file"], 'r') as f:
            self.password = f.read()
        self.ensure_schema()
        self.conn = psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            dbname=self.config["dbname"],
            password=self.password,
            sslmode='require')
        self.cursor = self.conn.cursor()

    def ensure_schema(self):
        """ Ensure that database, schema, and tables are created
        """
        with psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                dbname=self.config["default_dbname"],
                password=self.password,
                sslmode='require') as bootstrap_connection:
            bootstrap_connection.autocommit = True
            cursor = bootstrap_connection.cursor()
            try:
                cursor.execute("CREATE DATABASE {config[dbname]};".format(config=self.config))
            except psycopg2.errors.DuplicateDatabase:
                logger.info("Database %s exists", self.config["dbname"])

        with psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                dbname=self.config["dbname"],
                password=self.password,
                sslmode='require') as bootstrap_connection:
            cursor = bootstrap_connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMP,
                url VARCHAR,
                status_code NUMERIC
            );""".format(table=self.table))

    def __call__(self, check_result: CheckResult):
        self.cursor.execute("""
            INSERT INTO {table} (timestamp, url, status_code)
            VALUES (to_timestamp({result.timestamp}), '{result.url}', {result.status_code})
            """.format(
                table=self.table,
                result=check_result))
