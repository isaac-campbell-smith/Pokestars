import json
import os
from collections import defaultdict
import pandas as pd
import numpy as np

class PokeData(): 
    """
    CLASS TO CREATE DATAFRAMES FOR 
    1. Total Battle Counts on Pokemon Showdown
    2. Pokemon Usage Rates
    3. Pokemon Abilities
    4. Pokemon Teammates 
    5. Pokemon Counters 
    6. Pokemon Natures 
    7. Pokemon Items
    8. Pokemon ID reference
    9. Nature ID reference
    10. Ability ID reference
    11. Item ID reference

    Parent class that gives some high-level functionality to parse 2 different dictionary structures
    Popularity child targets one sublevel for one the total number of users that month
    Usage child targets complete matchup data
    """
    def __init__(self, columns=[]):
        self.columns = columns
        self.df = pd.DataFrame(columns=columns)  
        
    def _add_new_data(**kwargs):
        #new_data = defaultdict(list)
        return
    
    def update_df(self, **kwargs):
        new_data = self._add_new_data(**kwargs)
        self.df = pd.concat([self.df, new_data], sort=False, ignore_index=True)
        
## MONTHLY POPULARITY EXTRACTION        
class Popularity(PokeData):
    """
    Specific to outer layer of JSON file containing all user statistics
    """
    def __init__(self, columns=['month', 'num_battles']):
        PokeData.__init__(self, columns)
        
    def _add_new_data(self, month=str, value=int):
        out = pd.DataFrame([[month, value]], columns=self.columns)
        return out

class PokeReference():
    """
    Counter, dictionary and df data storage Class to track reference codes for database values
        Used within Usage Class (pokemon, abilities, natures, items)   
    INPUT
        dict_label => str, the target data key for the Showdown JSON file
        id_label => str 
            (i.e. 'Abilities' & 'ability_id')
    """
    def __init__(self, dict_label=None, id_label='id', columns=[]):
        self.next_id = 0
        self.info = dict()
        self.dict_label = dict_label 
        self.id_label = id_label
        self.df = pd.DataFrame(columns=columns)
        
    def update_ids(self, name):
        """
        Method checks if name is not in self.info (id reference dictionary)
        & Updates accordingly
        """
        if name not in self.info:
            self.info[name] = self.next_id
            self.next_id += 1  
    
    def update_df(self):
        self.df = pd.concat([self.df, pd.DataFrame(self.new_data)],
                             sort=False, ignore_index=True)
        return
            
    def _initialize_new_data(self):
        """
        Initializes a new object to clean/store a new month's worth of data
        """
        self.new_data = defaultdict(list)
        return
    
    def _add_new_data(self, month, dic, id_=None, key=None):
        """
        Method to update self.new_data object
        INPUT
            month => str
            dic => dict containing nebulus usage data on a single pokemon
            id_ => int or none, relating to outer for/loop of data cleaning 
            key => str or none, relating to outer for/loop for data cleaning
        OUTPUT
            None if id_ is given, otherwise id_ (int) 
            *IMPORTANT* id_ should only passed when using this Class for Pokemon 
            as this value is what ties the whole dataset together
        """
        if id_ != None:
            if self.dict_label == 'Spreads':
                #transform spreads into simple count of natures
                sub_dict = defaultdict(int)
                for spread, count in dic[self.dict_label].items():
                    nature = spread.split(':')[0]
                    self.update_ids(nature)
                    sub_dict[nature] += count
                dic[self.dict_label] = sub_dict
                    
            for item, count in dic[self.dict_label].items():
                self.update_ids(item)
                self.new_data['id'].append(id_)
                self.new_data[self.id_label].append(self.info[item])
                self.new_data['count'].append(count)
                self.new_data['month'].append(month)
            return
            
        else:
            self.update_ids(key)
            id_ = self.info[key] #primary pokemon key
            self.new_data['id'].append(id_)
            self.new_data['month'].append(month) 
            self.new_data['count'].append(dic['Raw count'])
            self.new_data['usage'].append(dic['usage'])
            return id_
            
    def pandify(self):
        df = pd.DataFrame(self.info.items(), columns=['name', 'id'])[['id', 'name']]
        return df
    
## MONTHLY POKEMON USAGE EXTRACTION   
class Usage(PokeData):
    """
    Child class that stores: 
        Pokemon usage data as it's primary df
        Teammate statistics as teammate_df
        Checks and counter statistics as counter_df
        Ability statistics as ability_df
        Spread statistics (parsed for only the ability name) as nature_df
        Reference dictionary storing unique keys for each Pokemon, Ability, Nature
    *NOTE* While the variable structure is a bit hairy, 
           it helps greatly reduce runtime
    """
    def __init__(self, columns=['id', 'month', 'count', 'usage']):
        PokeData.__init__(self, columns)

        self.ids = PokeReference()
        self.abilities = PokeReference(dict_label='Abilities', id_label='ability_id',
                                       columns=['id', 'ability_id', 'count', 'month'])
        self.items = PokeReference(dict_label='Items', id_label='item_id',
                                   columns=['id', 'item_id', 'count', 'month'])
        self.natures = PokeReference(dict_label='Spreads', id_label='nature_id',
                                     columns=['id', 'nature_id', 'count', 'month'])
        
        self.counter_df = pd.DataFrame(columns=['id', 'counter_id', 'num_battles', 'check_pct', 'month'])
        self.counter_func = lambda dic, k, v: k in dic.keys() and v[1] > 0.5
        
        self.teammate_df = pd.DataFrame(columns=['id', 'mate_id', 'x', 'month'])
        self.teammate_func = lambda dic, k, v: k in dic.keys() and dic[k]['usage'] > 0.005
        
    def _combine_data(self, new_counters, new_teammates):
        self.counter_df = pd.concat([self.counter_df, pd.DataFrame(new_counters)],
                                    sort=False, ignore_index=True)
        self.teammate_df = pd.concat([self.teammate_df, pd.DataFrame(new_teammates)],
                                     sort=False, ignore_index=True)
        self.abilities.update_df()
        self.items.update_df()
        self.natures.update_df()          
        out = pd.DataFrame(self.ids.new_data)
        
        return out
    
    
    def _add_new_counter_mates(self, month, dic, new_data, id_, key, label, func, check):
        for k, v in dic[key].items():
            if func(check, k, v):
                self.ids.update_ids(k)
            else:
                continue
            new_data['id'].append(id_)
            new_data[label].append(self.ids.info[k])
            if label == 'counter_id':
                new_data['num_battles'].append(v[0])
                new_data['check_pct'].append(v[1])
            else:
                new_data['x'].append(v)
            new_data['month'].append(month)
        return new_data
    
    def _add_new_data(self, dic=dict, month=str):
        
        self.ids._initialize_new_data()
        self.abilities._initialize_new_data()
        self.items._initialize_new_data()
        self.natures._initialize_new_data()
        new_counters = defaultdict(list)
        new_teammates = defaultdict(list)

        for key, sub in dic.items():
            #the dictionary to be passed in uses pokemon names as keys
            if sub['usage'] < 0.005:
                continue
                
            id_ = self.ids._add_new_data(month, sub, key=key)

            self.abilities._add_new_data(month, sub, id_=id_)
            self.items._add_new_data(month, sub, id_=id_)
            self.natures._add_new_data(month, sub, id_=id_)
            
            new_counters = self._add_new_counter_mates(month, sub, new_counters,
                                                       id_, 'Checks and Counters',
                                                       'counter_id', self.counter_func,
                                                       dic)
                
            new_teammates = self._add_new_counter_mates(month, sub, new_teammates,
                                                        id_, 'Teammates', 
                                                        'mate_id', self.teammate_func,
                                                        dic)
        
        return self._combine_data(new_counters, new_teammates)

def save_to_folder(df, wp, name):
    full_path = f'{wp}{name}.csv'
    df.to_csv(full_path, header=True, index=False)
    return

if __name__ == '__main__':

    popularity = Popularity()
    usage = Usage()
    
    read_folder = '~/Pokestars/data/raw/chaos/'
    
    for fp in sorted(os.listdir(read_folder)):
        try: #this try, except just helps to ignore any '.' sys files 
            with open(read_folder+fp, 'r') as f:
                d = json.load(f)
            month = fp[-7:-5] + '-' + fp[:4]
            print(f'\n>> Parsing Pokemon data from {month}')    
            dic = d['data']
            num_battles = d['info']['number of battles']

            popularity.update_df(month=month, value=num_battles)
            usage.update_df(dic=dic, month=month)
        except:
            pass
        
    write_folder = 'data/clean/'
    try:
        os.mkdir(write_folder)
    except:
        pass
    
    df_list = [ref.pandify() for ref in [usage.ids, usage.natures, 
                                         usage.abilities, usage.items]]
    df_list.extend([popularity.df, usage.df, usage.abilities.df, usage.items.df, 
                    usage.natures.df, usage.counter_df, usage.teammate_df])
    df_names = [name+'_reference' for name in ['pokemon', 'nature', 'ability', 'item']]
    df_names.extend(['monthly_popularity', 'battle_counts', 'ability_counts', 
                     'item_counts', 'nature_counts', 'counter_stats', 'teammate_stats'])
    
    for df, name in zip(df_list, df_names):
        save_to_folder(ref, write_folder, name)