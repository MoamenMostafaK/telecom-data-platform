# main.py
import sys
import os

# Ensure the root app directory is in the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fakerScript import generate_telecom_data       

if __name__ == "__main__":
    print("Starting data generation...")    
    generate_telecom_data()
    print("Data created execution successfully!")
