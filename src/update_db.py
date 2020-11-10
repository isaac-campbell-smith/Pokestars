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

class PokeData():
    def __init__(self, table_name, update_str):
        self.stats = defaultdict()
        self.table = table_name
        self.update_str = update_str

    def _db_check(self, key, cur, auto_update=True):
        if key in self.stats:
            id_ = self.stats[key]

        else:
            if self.table == 'pokemon':
                name = key.split("'")[0]
            else:
                name = key

            new_value_check = "SELECT id FROM {} WHERE name LIKE '{}%'".format(self.table, name)
            cur.execute(new_value_check)
            id_ = cur.fetchone()

            if id_ == None and auto_update:
                cur.execute("SELECT MAX(id) FROM {};".format(self.table))
                id_ = cur.fetchone()[0] + 1                
                sql_insert = "INSERT INTO {} {} VALUES ({}, '{}'); COMMIT;".format(self.table, self.update_str, id_, key)
                cur.execute(sql_insert)
                print ("NEW [{}] VALUE:".format(self.table), id_, key)

                try:
                    if self.table == 'pokemon':
                        update_str = new_poke_update(id_, poke)
                        cur.execute(update_str)
                except:
                    print ("FAILED TO UPDATE DB WITH SEREBII DATA")

            else:
                id_ = id_[0]

            self.stats[key]=id_

        return id_

class sqlWorker():
    def __init__(self, month):
        self.month = month
        self.pw = pw
        self.c = 0
        self.usage = PokeData('pokemon', '(id, name)')
        self.abilities = PokeData('abilities', '') 
        self.natures = PokeData('natures', '')
        self.items = PokeData('items', '')

    def _get_data(self):
        """
        INPUT: 
            month: string = YYYY-MM, i.e. 2020-08
        """
        url = f'https://www.smogon.com/stats/{self.month}/chaos/gen8ou-0.json'
        r = requests.get(url)
        content = json.loads(r.content)
        self.data = content
        self.dic = content['data']
        self.total_pokemon = len(self.dic.keys())
        self.num_battles = content['info']['number of battles']
        self.month = self.month[-2:] + '-' + self.month[:4]
        return

    def _connect_to_db(self):
        self.conn = psycopg2.connect(self.pw)
        self.cur = self.conn.cursor()
        self.conn.rollback()
        return

    def _upload(self, table_name, v1, v2, v3, v4):
        sql = "INSERT INTO " + table_name 
        sql += " VALUES ({}, {}, {}, '{}'); COMMIT;".format(v1, v2, v3, v4)
        self.cur.execute(sql)
        return

    def extract_data(self):
        print ('Fetching Data \n')
        self._get_data()
        self._connect_to_db()
        print ('Connected to DataBase Server \n')

        sql = "INSERT INTO users VALUES ('{}', {}); COMMIT;".format(self.month, self.num_battles)
        self.cur.execute(sql)
        for key, sub in self.dic.items():
            if sub['usage'] < 0.005:
                continue
            id_ = self.usage._db_check(key, self.cur)
            sql = "INSERT INTO battles VALUES ({}, '{}', {}, {}); COMMIT;".format(id_, self.month, sub['Raw count'], sub['usage'])
            self.cur.execute(sql)
            
            for ability, count in sub['Abilities'].items():
                ability_id = self.abilities._db_check(ability, self.cur)
                self._upload('battle_abilities', id_, ability_id, count, self.month)

            for item, count in sub['Items'].items():
                item_id = self.items._db_check(item, self.cur, auto_update=False)
                self._upload('battle_items', id_, item_id, count, self.month)

            sub_nature_vals = defaultdict(int)
            for spread, count in sub['Spreads'].items():
                nature = spread.split(':')[0]
                sub_nature_vals[nature] += count
            for nature, count in sub_nature_vals.items():
                nature_id = self.natures._db_check(nature, self.cur, auto_update=False)
                self._upload('battle_natures', id_, nature_id, count, self.month)

            for counter, arr in sub['Checks and Counters'].items():
                if counter in self.dic.keys() and arr[1] > 0.5 and self.dic[counter]['usage'] > 0.005:
                    counter_id = self.usage._db_check(counter, self.cur)
                else:
                    continue
                sql = "INSERT INTO counters VALUES ({}, {}, {}, {}, '{}'); COMMIT;".format(id_, counter_id, arr[0], arr[1], self.month)
                self.cur.execute(sql)

            for mate, x in sub['Teammates'].items():
                if mate in dic.keys() and dic[mate]['usage'] > 0.005:
                    mate_id = self.usage._db_check(mate, self.cur)
                else:
                    continue
                self._upload('teammates', id_, mate_id, x, self.month)

            print (f'{key} data has been updated | {self.c}/{self.total_pokemon} complete')
            self.c += 1

        self.cur.close()
        self.conn.close()

if __name__ == '__main__':
    with open('src/pw') as pw_file:
        pw = pw_file.readline()

    worker = sqlWorker('2020-10', pw)
    worker.extract_data()
    print (f'DB HAS BEEN UPDATED FOR THE CURRENT MONTH WITH {worker.num_battles} battles')