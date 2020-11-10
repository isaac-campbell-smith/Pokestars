import pandas as pd
import psycopg2
import os
from pySQL_funcs import pretty_query

with open('pw') as pw_file:
    pw = pw_file.readline()

def pull_load_latest_data(conn, cur, date):
    """
    Connect to database and write the latest month's data to csv/s
    """
    tables = ['users', 'battles', 'teammates', 'counters', 
              'battle_abilities', 'battle_natures', 'battle_items']
    os.mkdir('../data/latest/'+date)
    for table in tables:
        query = """SELECT * FROM {} WHERE month='{}';""".format(table, date)
        data = pretty_query(cur, query, conn)
        print (data)
        write_file = f'../data/latest/{date}/{date}_{table}.csv'
        data.to_csv(write_file)
    return

if __name__ == '__main__':
    date='09-2020'
    conn = psycopg2.connect(pw)
    cur = conn.cursor()
    print (pull_load_latest_data(conn, cur, date))
    cur.close()
    conn.close()
    
