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
    Popularity child targets one sublevel for one t
    Usage child 
    """
    def __init__(self, columns=[]):
        self.columns = columns
        self.df = pd.DataFrame(columns=columns)  
        
    def _add_new_data(**kwargs):
        return
    
    def update_df(self, **kwargs):
        new_data = self._add_new_data(**kwargs)
        self.df = pd.concat([self.df, new_data], sort=False, ignore_index=True)
        
## MONTHLY POPULARITY EXTRACTION        
class Popularity(PokeData):
    def __init__(self, columns=['month', 'num_battles']):
        PokeData.__init__(self, columns)
        
    def _add_new_data(self, month=str, value=int):
        out = pd.DataFrame([[month, value]], columns=self.columns)
        return out
    
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
        self.ids = dict()
        self.abilities = dict()
        self.natures = dict()
        self.item = dict()
        
        self.next_id = 0 #tracks the next Pokemon id to store
        self.next_ab = 0            #next Ability id to store
        self.next_nat = 0            #next Nature id to store
        
        self.teammate_df = pd.DataFrame(columns=['id', 'mate_id', 'x', 'month'])
        self.counter_df = pd.DataFrame(columns=['id', 'counter_id', 'num_battles', 'check_pct', 'month'])
        self.ability_df = pd.DataFrame(columns=['id', 'ability_id', 'count', 'month'])
        self.nature_df = pd.DataFrame(columns=['id', 'nature_id', 'count', 'month'])
        self.items_df = pd.DataFrame(columns=['id', 'item_id', 'count', 'month'])
        
    def _update_ids(self, name):
        """
        Check if the name is in a dictionary & return it's value. 
        Otherwise update the dictionary & current count accordingly.
        INPUT:
            dic : dictionary
            x   : int
            name: string
        """ 
        if name not in self.ids:
            self.ids[name] = self.next_id
            self.next_id += 1
            
    def _update_abilities(self, name):
        """
        Check if the name is in a dictionary & return it's value. 
        Otherwise update the dictionary & current count accordingly.
        INPUT:
            dic : dictionary
            x   : int
            name: string
        """ 
        if name not in self.abilities:
            self.abilities[name] = self.next_ab
            self.next_ab += 1
            
    def _update_natures(self, name):
        """
        Check if the name is in a dictionary & return it's value. 
        Otherwise update the dictionary & current count accordingly.
        INPUT:
            dic : dictionary
            x   : int
            name: string
        """         
        if name not in self.natures:
            self.natures[name] = self.next_nat
            self.next_nat += 1
            
    def _update_items(self, name):
        """
        Check if the name is in a dictionary & return it's value. 
        Otherwise update the dictionary & current count accordingly.
        INPUT:
            dic : dictionary
            x   : int
            name: string
        """         
        if name not in self.item:
            self.item[name] = self.next_ite
            self.next_ite += 1     
            
    def _add_new_data(self, dic=dict, month=str, targets=dict):
        out = defaultdict(list) #new dictionary for usage status
        
        new_ability = defaultdict(list) #ability stats
        new_nature = defaultdict(list) #
        new_item = defaultdict(list)
        new_counters = defaultdict(list)
        new_teammates = defaultdict(list)
        
        for key, sub in dic.items():
            #the dictionary to be passed in uses pokemon names as keys
            if sub['usage'] < 0.005:
                continue
                
            self._update_ids(key)
            id_ = self.ids[key] #primary pokemon key
            out['id'].append(id_)
            out['month'].append(month) 
            
            for k, v in targets.items():
                out[k].append(sub[v])
                
            for ability, count in sub['Abilities'].items():
                self._update_abilities(ability)
                new_ability['id'].append(id_) #primary pokemon key
                new_ability['ability_id'].append(self.abilities[ability])
                new_ability['count'].append(count)
                new_ability['month'].append(month)
                
            for item, count in sub['Items'].items():
                self._update_items(item)
                new_item['id'].append(id_)
                new_item['item_id'].append(self.item[item])
                new_item['count'].append(count)
                new_item['month'].append(month)
            
            sub_nature_vals = defaultdict(int)
            
            for spread, count in sub['Spreads'].items():
                nature = spread.split(':')[0]
                self._update_natures(nature)
                sub_nature_vals[nature] += count
                
            for nature, count in sub_nature_vals.items():
                new_nature['id'].append(id_)
                new_nature['nature_id'].append(self.natures[nature])
                new_nature['count'].append(count)
                new_nature['month'].append(month)
                
            for counter, arr in sub['Checks and Counters'].items():
                if counter in dic.keys() and arr[1] > 0.5:
                    self._update_ids(counter)
                else:
                    continue
                new_counters['id'].append(id_)
                new_counters['counter_id'].append(self.ids[counter])
                new_counters['num_battles'].append(arr[0])
                new_counters['check_pct'].append(arr[1])
                new_counters['month'].append(month)
                
            for mate, x in sub['Teammates'].items():
                if mate in dic.keys() and dic[mate]['usage'] > 0.005:
                    self._update_ids(mate)
                else:
                    continue
                new_teammates['id'].append(id_)
                new_teammates['mate_id'].append(self.ids[mate])
                new_teammates['x'].append(x)
                new_teammates['month'].append(month)
                
        self.teammate_df = pd.concat([self.teammate_df, 
                                     pd.DataFrame(new_teammates)], 
                                     sort=False, ignore_index=True)
        
        self.counter_df = pd.concat([self.counter_df, 
                                     pd.DataFrame(new_counters)], 
                                     sort=False, ignore_index=True)
                        
        self.ability_df = pd.concat([self.ability_df, 
                                     pd.DataFrame(new_ability)], 
                                     sort=False, ignore_index=True)
        
        self.nature_df = pd.concat([self.nature_df, 
                                     pd.DataFrame(new_nature)], 
                                     sort=False, ignore_index=True)

        self.items_df = pd.concat([self.items_df,
                                   pd.DataFrame(new_item)],
                                   sort=False, ignore_index=True)
        
        out = pd.DataFrame(out)

        return out

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
            usage.update_df(dic=dic, month=month, targets={'count':'Raw count',
                                                           'usage':'usage'})
        except:
            pass
        
    write_folder = '~/Pokestars/data/clean/'
    
    pokemon_ref = pd.DataFrame(usage.ids.items(), columns=['name', 'id'])[['id', 'name']]
    nature_ref = pd.DataFrame(usage.natures.items(), columns=['name', 'id'])[['id', 'name']]
    ability_ref = pd.DataFrame(usage.abilities.items(), columns=['name', 'id'])[['id', 'name']]
    item_ref = pd.DataFrame(usage.item.items(), columns=['name', 'id'])[['id', 'name']]
    
    pokemon_ref.to_csv(write_folder+'pokemon_reference.csv', header=True, index=False)
    nature_ref.to_csv(write_folder+'nature_reference.csv', header=True, index=False)
    ability_ref.to_csv(write_folder+'ability_reference.csv', header=True, index=False)
    item_ref.to_csv(write_folder+'item_reference.csv', header=True, index=False)
    popularity.df.to_csv(write_folder+'monthly_popularity.csv', header=True, index=False)
    usage.df.to_csv(write_folder+'battle_counts.csv', header=True, index=False)
    usage.nature_df.to_csv(write_folder+'nature_counts.csv', header=True, index=False)
    usage.ability_df.to_csv(write_folder+'ability_counts.csv', header=True, index=False)
    usage.items_df.to_csv(write_folder+'item_counts.csv', header=True, index=False)
    usage.teammate_df.to_csv(write_folder+'teammate_stats.csv', header=True, index=False)
    usage.counter_df.to_csv(write_folder+'counter_stats.csv', header=True, index=False)