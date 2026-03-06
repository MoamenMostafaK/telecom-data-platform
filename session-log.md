Day 1,2,3:
Fixed Project Structure, Figured our docker-compose.yml (python app + postgresql + adminer) and Dockerfile (python app for now to generate the data), script run in python container successfuly and faker data is loaded Generated 2000 customers and 12000 CDR records, did some queries on DBeaver and saved them in /sql/querries.sql
PS: volumes and file managment in this project turned out to be a thing i should learn more specially with docker and WSL

Day 4:

## First ETL

Built a Faker-based data generator to create telecom sample datasets
(customers, plans, CDR, network events) as CSVs into data/raw.
Implemented a Python ETL script using psycopg2 that reads these CSV files from
data/raw and inserts them into PostgreSQL tables.
Key features:

- CSV ingestion using Python csv module / library
- Data cleaning (empty values converted to NULL)
- Correct load order respecting foreign key constraints
- End-to-end pipeline verified by querying row counts
  Pipeline runs successfully and loads sample data into PostgreSQL.
