from bs4 import BeautifulSoup
from collections import defaultdict
import requests
import json

import pandas as pd
import psycopg2

from json_clean_to_csv import *

def get_data(month):
    """
    INPUT: 
        month: string = YYYY-MM, i.e. 2020-08
    """
    url = f'https://www.smogon.com/stats/{month}/chaos/gen8ou-0.json'
    r = requests.get(url)
    content = r.content
    data = json.loads(content)
    return data

def new_poke_update(id_, poke):
    """
    Pulls new Pokemon stats from Serebii & concatenates data into sql string
    """
    base_url = 'https://serebii.net/pokedex-swsh/'
    
    sql = "UPDATE pokemon SET (type_1, type_2, attack, defense, sp_attack, sp_defense, speed) "

    try:
        url = base_url + poke.lower().split('-')[0] + '/'
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html')
        dextable = soup.find_all('table', {'class':'dextable'})
        
        #Checks whether this is a base forme
        forme_check = lambda s: 'Galar' in s or 'Mega' in s or 'Alola' in s
        
        if forme_check(poke):
            stat_idx = 1
            type_grab = False
        else:
            stat_idx = 0 
            type_grab = True
            
        #Checks for Pokemon w/ Multiple Formes
        if dextable[1].find('td', {'class':'cen'}).find_all('td'):
            #Boolean trigger for grabbing 1 of 2 type pairs
            alt_check = lambda s: 'Normal' not in s and '\n' not in s  
            for section in dextable[1].find('td', {'class':'cen'}).find_all('td'):
                if type_grab and '\n' in section.text:
                    types = [a.find('img')['alt'].replace('-type', '') for a in section.find_all('a')]
                    break
                if alt_check(section.text):
                    type_grab = True
        else: 
            types = [td.find('img')['alt'].replace('-type', '') for td in dextable[1].find_all('a')]
        
        if len(types) == 1:
            types.append('None')
        
        #EXTRACT STATS TOTALS (NOTICE STAT_IDX)
        base_stats = [t.text for t in dextable if 'Base Stats - Total' in t.text]
        hp, at, df, spa, spd, spe = [int(stat) for stat in base_stats[stat_idx].split('Base Stats - Total: ')[1].split('\n')[1:7]]

        #COMPLETE SQL STRING
        sql += "VALUES ('{}', '{}', ".format(types[0], types[1])
        sql +=  "{}, {}, {}, {}, {}, {}) WHERE id = {}; COMMIT;".format(hp, at, df, spa, spd, spe, id_)

        print (types)
        print (hp, at, df, spa, spd, spe)
    except:
        print ("FAILED TO COLLECT DATA!!")

    return sql

def json_extract(month, sql_str):
    """
    FUNCTION TO LOOP THROUGH DICTIONARY-STRUCTURED BATTLING DATA 
    & WRITE TO EACH DATABASE TABLE LINE-BY-LINE
    Author's Note to self: Much of this code is borrowed from the original data clean script and
                 can be modularized further. Writing to a database line-by-line is also very slow.
                 Future refactoring to-do list:
                    1. TRY TO FIGURE OUT HOW TO, IF POSSIBLE, UPDATE A TABLE WITH A CSV
                    2. Modularize if statements that trigger writing new information to the database
                    3. Grab all reference tables upfront (rather than iteratively for each)
                    4. Figure out how to enter month from the command line
                    5. Auto update new Pokemon stats
    """
    data = get_data(month)
    dic = data['data']
    month = month[-2:] + '-' + month[:4]
    num_battles = data['info']['number of battles']

    #DICTIONARIES TO TRACK WHETHER WE'VE ALREADY QUERIED A PARTICULAR ID
    new_usage = defaultdict() 
    new_ability = defaultdict() 
    new_nature = defaultdict()
    new_item = defaultdict()

    conn = psycopg2.connect(sql_str)
    cur = conn.cursor()
    conn.rollback()

    users_insert = "INSERT INTO users VALUES ('{}', {}); COMMIT;".format(month, num_battles)
    cur.execute(users_insert)
    
    print ('Fetching Data')
    total_pokemon = len(dic.keys())
    c = 0
    for key, sub in dic.items():
        if sub['usage'] < 0.005:
            continue
        
        if key in new_usage:
            id_ = new_usage[key]
        else:
            name = key.split("'")[0]
            new_poke_check = "SELECT id FROM pokemon WHERE name LIKE '{}%'".format(name)
            cur.execute(new_poke_check)
            id_ = cur.fetchone()

            if id_ == None:
                cur.execute("SELECT MAX(id) FROM pokemon;")
                id_ = cur.fetchone()[0] + 1
                
                pokemon_insert = "INSERT INTO pokemon (id, name) VALUES ({}, '{}'); COMMIT;".format(id_, key)
                cur.execute(pokemon_insert)
                print ("NEW POKEMON:", id_, key)
                try:
                    update_str = new_poke_update(id_, poke)
                    cur.execute(update_str)
                except:
                    pass
            else:
                id_ = id_[0]
            new_usage[key]=id_

        pokemon_insert = "INSERT INTO battles VALUES ({}, '{}', {}, {}); COMMIT;".format(id_, month, sub['Raw count'], sub['usage'])
        cur.execute(pokemon_insert)

        for ability, count in sub['Abilities'].items():
            if ability in new_ability:
                ability_id = new_ability[ability]
            else:
                new_ability_check = "SELECT id FROM abilities WHERE name='{}'".format(ability)
                cur.execute(new_ability_check)
                ability_id = cur.fetchone()

                if ability_id == None:
                    cur.execute("SELECT MAX(id) FROM abilities;")
                    ability_id = cur.fetchone()[0] + 1
                    ability_insert = "INSERT INTO abilities VALUES ({}, '{}'); COMMIT;".format(ability_id, ability)
                    cur.execute(ability_insert)
                    print ("NEW ABILITY: ", ability_id, ability)
                else:
                    ability_id = ability_id[0]
                new_ability[ability] = ability_id
            ability_insert = "INSERT INTO battle_abilities VALUES ({}, {}, {}, '{}'); COMMIT;".format(id_, ability_id, count, month)
            cur.execute(ability_insert)

        for item, count in sub['Items'].items():
            if item in new_item:
                item_id = new_item[item]
            else:
                new_item_check = "SELECT id FROM items WHERE name='{}'".format(item)
                cur.execute(new_item_check)
                item_id = cur.fetchone()

                if item_id == None:
                    continue
                else:
                    item_id = item_id[0]
                new_item[item] = item_id
            item_insert = "INSERT INTO battle_items VALUES ({}, {}, {}, '{}'); COMMIT;".format(id_, item_id, count, month)
            cur.execute(item_insert)

        sub_nature_vals = defaultdict(int)
        for spread, count in sub['Spreads'].items():

            nature = spread.split(':')[0]
            if nature in new_nature:
                nature_id = new_nature[nature]
            else:
                new_nature_check = "SELECT id FROM natures WHERE name='{}'".format(nature)
                cur.execute(new_nature_check)
                nature_id = cur.fetchone()[0]
                new_nature[nature] = nature_id
            sub_nature_vals[nature] += count
        
        for nature, count in sub_nature_vals.items():
            nature_insert= "INSERT INTO battle_natures VALUES ({}, {}, {}, '{}'); COMMIT;".format(id_, new_nature[nature], count, month)
            cur.execute(nature_insert)

        for counter, arr in sub['Checks and Counters'].items():
            if counter in dic.keys() and arr[1] > 0.5 and dic[counter]['usage'] > 0.005:
                if counter in new_usage:
                    counter_id = new_usage[counter]
                else:
                    name = counter.split("'")[0]
                    new_counter_check = "SELECT id FROM pokemon WHERE name LIKE '{}%'".format(name)
                    cur.execute(new_counter_check)
                    counter_id = cur.fetchone()

                    if counter_id == None:
                        cur.execute("SELECT MAX(id) FROM pokemon;")
                        counter_id = cur.fetchone()[0] + 1
                        pokemon_insert = "INSERT INTO pokemon (id, name) VALUES ({}, '{}'); COMMIT;".format(counter_id, counter)
                        cur.execute(pokemon_insert)
                        print ("NEW POKEMON:", counter_id, counter)
                    else:
                        counter_id = counter_id[0]
                    new_usage[counter] = counter_id
            else:
                continue

            counter_insert = "INSERT INTO counters VALUES ({}, {}, {}, {}, '{}'); COMMIT;".format(id_, counter_id, arr[0], arr[1], month)
            cur.execute(counter_insert)
                
        for mate, x in sub['Teammates'].items():
            if mate in dic.keys() and dic[mate]['usage'] > 0.005:
                if mate in new_usage:
                    mate_id = new_usage[mate]
                else:
                    name = mate.split("'")[0]
                    new_mate_check = "SELECT id FROM pokemon WHERE name LIKE '{}%'".format(name)
                    cur.execute(new_mate_check)
                    mate_id = cur.fetchone()

                    if mate_id == None:
                        cur.execute("SELECT MAX(id) FROM pokemon;")
                        mate_id = cur.fetchone()[0] + 1
                        pokemon_insert = "INSERT INTO pokemon (id, name) VALUES ({}, '{}'); COMMIT;".format(mate_id, mate)
                        cur.execute(pokemon_insert)
                        print ("NEW POKEMON: ", mate_id, mate)
                    else:
                        mate_id = mate_id[0]
                    new_usage[mate] = mate_id
            else:
                continue
            mate_insert = "INSERT INTO teammates VALUES ({}, {}, {}, '{}'); COMMIT".format(id_, mate_id, x, month)
            cur.execute(mate_insert) 
        #THIS PRINT STATEMENT WILL LIKELY NEVER GO TO '100%' COMPLETE AS WE PASS OVER NEGIBLE POKEMON USAGE
        print (f'{key} data has been updated | {c}/{total_pokemon} complete')
        c += 1
    conn.close()

    return num_battles

if __name__ == '__main__':
    with open('src/pw') as pw_file:
        pw = pw_file.readline()
    num_battles = json_extract('2020-10', pw)
    print (f'DB HAS BEEN UPDATED FOR THE CURRENT MONTH WITH {num_battles} battles')