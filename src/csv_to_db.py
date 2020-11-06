"""
This is python script streams s3 bucket data (csv's) into the AWS RDS postgres database
It serves to launch the initial database in conjunction with scrape and json clean scripts
"""
import psycopg2 
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from io import StringIO

def transfer_data(pw, table_dict):
    """
    Function to transfer S3 Bucket Data to SQL RDS
    """
    #CONNECT TO DB (MODIFY STRING PARAMETER AS NEEDED)
    conn = psycopg2.connect(pw)
    cur = conn.cursor()
    #(cur.execute('DROP DATABASE IF EXISTS pokestars;'))

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute(sql.SQL("CREATE DATABASE pokestars").format(
            sql.Identifier(self.db_name))
        )
    #create = 'BEGIN; CREATE DATABASE pokemon; COMMIT;'
    #cur.execute(create) #COMMENT THIS LINE OUT IF ON AWS
    for key, item in table_dict.items():

        name, schema = item

        create_query = f'CREATE TABLE {name} {schema}; COMMIT;' 
        cur.execute(create_query)

        #ANOTHER PARAMETER UNIQUE TO THIS DB
        data_endpoint = f'https://c-smith1.s3-us-west-1.amazonaws.com/pokemon/{key}.csv'
        df = pd.read_csv(data_endpoint)
        columns = df.columns

        f = StringIO() #temporary housing object for sql compatibility
        df.to_csv(f, index=False, header=False)

        f.seek(0) #initialize the IO cursor
        cur.copy_from(f, name, columns=columns, sep=',')
        conn.commit()

        print (f'{name} successfully created')

    cur.close()
    conn.close()
        
if __name__ == '__main__':
    with open('src/pw') as pw_file:
        pw = pw_file.readline()

    #DICTIONARY TO MATCH FILE NAMES TO DESIRED TABLE NAME & SCHEMA
        #keys correspond to AWS S3 bucket file names
        #values: list, index [0] = sql table name, [1] = schema
    table_dict = {
        'pokemon_withtypes_reference':['pokemon', 
                            '(id INTEGER, name VARCHAR, type_1 VARCHAR, type_2 VARCHAR)'],
        'ability_reference':['abilities', 
                            '(id INTEGER, name VARCHAR)'],
        'nature_reference':['natures', 
                            '(id INTEGER, name VARCHAR)'],
        'item_reference':['items',
                        '(id INTEGER, name VARCHAR)'],
        'battle_counts':['battles', 
                        '(id INTEGER, month VARCHAR, count INTEGER, usage REAL)'],
        'nature_counts':['battle_natures', 
                        '(id INTEGER, nature_id INTEGER, count REAL, month VARCHAR)'],
        'ability_counts':['battle_abilities',
                        '(id INTEGER, ability_id INTEGER, count REAL, month VARCHAR)'],
        'item_counts':['battle_items',
                    '(id INTEGER, item_id INTEGER, count REAL, month VARCHAR)'],
        'teammate_stats':['teammates', 
                        '(id INTEGER, mate_id INTEGER, x REAL, month VARCHAR)'],
        'counter_stats':['counters', 
                        '(id INTEGER, counter_id INTEGER, num_battles REAL, check_pct REAL, month VARCHAR)'],
        'monthly_popularity':['users',
                            '(month VARCHAR, num_battles INTEGER)']
    }

    transfer_data(pw, table_dict)