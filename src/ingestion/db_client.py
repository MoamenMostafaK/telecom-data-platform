# src/ingestion/db_client.py
import psycopg2
from config.db_config import DB_CONFIG

class PostgresClient:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def fetchall(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

    def close(self):
        self.cur.close()
        self.conn.close()
