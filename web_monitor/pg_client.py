import logging
import datetime
import psycopg2

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PgClient():
    def __init__(self, pg_config):
        self.config = pg_config
        self.table = "check_result"

        with open(self.config["pw_file"], 'r') as f:
            self.password = f.read()
        self._ensure_schema()

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            dbname=self.config["dbname"],
            password=self.password,
            sslmode='require')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()

    def _ensure_schema(self):
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
                timestamp TIMESTAMP NOT NULL,
                url VARCHAR NOT NULL,
                status_code NUMERIC NOT NULL,
                response_time NUMERIC NOT NULL,
                match_content BOOLEAN,
                PRIMARY KEY (timestamp, url)
            );""".format(table=self.table))

    def __call__(self, result_list):
        self.cursor.executemany(
            """INSERT INTO {table} (timestamp, url, status_code, response_time, match_content)
               VALUES (%s, %s, %s, %s, %s);""".format(table=self.table),
            ((
                datetime.datetime.fromtimestamp(result.timestamp),
                result.url,
                result.status_code,
                result.response_time,
                result.match_content
            ) for result in result_list)
        )
