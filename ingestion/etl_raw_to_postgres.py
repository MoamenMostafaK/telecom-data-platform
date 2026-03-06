import psycopg2
import csv















def connect_and_ingest():
    try:
        # Separate host and port into distinct arguments
        conn = psycopg2.connect(
            host="localhost", 
            port="5498", 
            database="telecomDB", 
            user="admin", 
            password="admin"
        )

        cursor = conn.cursor()

        # Using status attribute to verify the connection
        print(f"Connection Status: {conn.status}")







        def load_csv_to_table(csv_path, table_name, columns):

            with open(csv_path, "r") as f:

                reader = csv.reader(f)
                next(reader)

                for row in reader:

                    placeholders = ",".join(["%s"] * len(columns))
                    column_list = ",".join(columns)

                    query = f"""
                    INSERT INTO {table_name} ({column_list})
                    VALUES ({placeholders})
                    """
                    clean_row = [None if value == "" else value for value in row]
                    cursor.execute(query, clean_row)

        
        
        
        
        
        load_csv_to_table(
            "data/raw/plans.csv",
            "public.plans",
            ["plan_id", "plan_name", "monthly_fee", "data_gb", "call_minutes"]
        )
        
        
        
        
        
        load_csv_to_table(
            "data/raw/customers.csv",
            "public.customers",
            ["customer_id", "name", "plan_id", "region", "signup_date", "churn_date"]
            )



        load_csv_to_table(
            "data/raw/cdr.csv",
            "public.cdr",
            ["record_id","caller_id","receiver_id","call_start","duration_sec","call_type","tower_id"]
        )

        load_csv_to_table(
            "data/raw/network_events.csv",
            "public.network_events",
            ["event_id", "tower_id", "event_type", "severity", "timestamp"]
        )            

        conn.commit()




        tables = [
            "public.customers",
            "public.plans",
            "public.cdr",
            "public.network_events"
        ]

        for table in tables:

            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            print(table, count)










        
        
        
        # Always close the connection
        cursor.close()
        conn.close()
    except Exception as e:
        print("An error occurred while connecting to the database:", e)
        conn.rollback()  

# THIS IS THE MISSING PART:
if __name__ == "__main__":
    connect_and_ingest()