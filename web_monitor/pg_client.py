import logging
import psycopg2
from web_monitor.check_result import CheckResult

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class PgClient():
    def __init__(self, connection_string, database, table):
        self.connection_string = connection_string
        self.database = database
        self.table = table

        self.conn = psycopg2.connect(
            connection_string)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.ensure_schema()
        

    def ensure_schema(self):
        """ Ensure that database, schema, and tables are created
        """
        try:
            self.cursor.execute("CREATE DATABASE {};".format(self.database))
        except psycopg2.errors.DuplicateDatabase:
            logger.info("Database %s exists", self.database)

        self.conn.close()
        self.conn = psycopg2.connect(self.connection_string, dbname=self.database)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS {table} (
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
