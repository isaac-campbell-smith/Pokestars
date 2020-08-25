import boto3
import psycopg2 
import pandas as pd
import io

s3 = boto3.client('s3')

response = s3.list_objects_v2(Bucket='c-smith1')

#GET ONLY THE FILE NAMES FROM THE S3 BUCKET THAT ARE RELEVANT
file_list = [obj['Key'].replace('pokemon/','') for obj in response['Contents']  
                       if 'pokemon/' in obj['Key'] and obj['Key'][-4:] == '.csv']

#DICTIONARY TO MATCH FILE NAMES TO DESIRED TABLE NAME & SCHEMA
    #keys correspond to file_list names
    #values: list, [0] index is table name, [1] is schema
table_dict = {
    'pokemon_reference':['pokemon', 
                         '(id INTEGER, name VARCHAR)'],
    'ability_reference':['abilities', 
                         '(id INTEGER, name VARCHAR)'],
    'nature_reference':['natures', 
                        '(id INTEGER, name VARCHAR)'],
    'battle_counts':['battles', 
                     '(id INTEGER, month VARCHAR, count INTEGER, usage REAL)'],
    'nature_counts':['battle_natures', 
                     '(id INTEGER, nature_id INTEGER, count REAL, month VARCHAR)'],
    'ability_counts':['battle_abilities',
                      '(id INTEGER, ability_id INTEGER, count REAL, month VARCHAR)'],
    'teammate_stats':['teammates', 
                      '(id INTEGER, mate_id INTEGER, x REAL, month VARCHAR)'],
    'counter_stats':['counters', 
                     '(id INTEGER, counter_id INTEGER, num_battles REAL, check_pct REAL, month VARCHAR)'],
    'monthly_popularity':['users',
                          '(month VARCHAR, num_battles INTEGER)']
}

#CONNECT TO DB
conn = psycopg2.connect('dbname=pokemon user=postgres password=password host=localhost')
cur = conn.cursor()

#ITERATE THROUGH FILE LAST
    #1. CONCAT file name & corresponding table values
    #2. EXECUTE & SAVE

#OUTPUT: tables inserted
for f in file_list:

    key = f[:-4].replace('pokemon/','')
    name, schema = table_dict[key]

    create = f'CREATE TABLE {name} {schema}; COMMIT;' 
    cur.execute(create)

    temp = io.StringIO()
    data_endpoint = f'https://c-smith1.s3-us-west-1.amazonaws.com/pokemon/{f}'
    df = pd.read_csv(data_endpoint)
    columns = df.columns
    df.to_csv(temp, index=False, header=False)

    temp.seek(0)
    cur.copy_from(temp, name, columns=columns, sep=',')
    conn.commit()

    print (f'Table: {name} successfully created')

cur.close()
conn.close()
        
