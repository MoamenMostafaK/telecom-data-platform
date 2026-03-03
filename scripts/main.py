# main.py

from src.transformation.transform import deduplicate_customers
from src.load.load import create_fact_calls

if __name__ == "__main__":
    deduplicate_customers()
    create_fact_calls()
    print("Pipeline executed successfully!")
