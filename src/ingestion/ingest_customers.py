import psycopg2
from config.db_config import DB_CONFIG

class CustomerIngestion:

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()

    def insert_customer(self, customer):
        query = """
            INSERT INTO raw.customers
            (customer_id, full_name, national_id, city, registration_date)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, customer)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
