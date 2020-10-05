import pandas as pd
import psycopg2

conn = psycopg2.connect('dbname=postgres user=postgres host=localhost port=5432 password=password')
cur = conn.cursor()
conn.rollback()
conn.close()

def pull_load_latest_data():
    """
    connect to database and write the latest month's data to csv/s
    """