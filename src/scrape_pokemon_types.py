from bs4 import BeautifulSoup
from collections import defaultdict
import requests
import time
import json
import pandas as pd


def get_pokemon_types(poke):
    url = "https://pokemondb.net/pokedex/" + poke
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html')
    vitals = soup.find('table', {'class':'vitals-table'})
    typeset = vitals.find_all('tr')[1].find_all('a')
    type_list = [typ.text for typ in typeset]
    return type_list

problem_pokes = {
    133:['Psychic','Fairy'],
    436:['Grass','Dragon'],
    441:['Dragon','Fairy'],
    445:['Normal','Fighting'],
    456:['Water','Dark'],
    457:['Bug','Flying'],
    460:['Steel'],
    469:['Electric','Dragon'],
    472:['Normal','Fairy'],
    515:['Ice','Steel'],
    517:['Psychic','Fairy'],
    518:['Electric','Fairy'],
    522:['Grass','Fairy'],
    524:['Water','Fairy'],
    540:['Dragon','Fighting'],
    528:['Electric','Psychic'],
    530:['Fire','Ghost'],
    537:['Poison','Dark'],
    538:['Ice','Fairy'],
    548:['Grass','Dragon'],
    562:['Normal'],
    574:['Ground','Steel'],
    575:['Dark'],
    577:['Rock','Electric'],
    583:['Dark','Normal'],
    598:['Dark','Normal'],
    601:['Dragon','Fighting'],
    631:['Ice', 'Psychic'],
    638:['Fighting'],
    634:['Poison','Fairy'],
    650:['Ice','Psychic'],
    652:['Ice'],
    653:['Psychic','Fairy'],
    678:['Ghost'],
    681:['Ground','Steel'],
    684:['Fighting'],
    690:['Dark','Normal'],
    697:['Psychic'],
    709:['Poison','Psychic'],
    708:['Fighting','Water']
}

if __name__ == '__main__':
    pokemon_df = pd.read_csv('~/Pokestars/data/clean/pokemon_reference.csv')

    pokemon_df['type_1'] = 'None'
    pokemon_df['type_2'] = 'None'

    for row in pokemon_df.iterrows():
        poke = row[1]['name'].lower().split('-')[0]
        try:
            type_list = get_pokemon_types(poke)

            pokemon_df['type_1'].loc[row[0]] = type_list[0]

            if len(type_list) > 1:
                pokemon_df['type_2'].loc[row[0]] = type_list[1]
        except:
            print (f'Pokemon: {row[1]["name"]} ID: {row[0]} FAILED')
            #THE PRINT STATEMENT SHOULD BE RESOLVED BY THE PROBLEM_POKES dictionary LOOP BELOW
            #IF AN ID THROWS AN ERROR NOT IN THE DICTIONARY IT IS PROBABLY A CONNECTION ISSUE

        time.sleep(8)
        if row[0] % 50 == 0:
            print (f'{row[0]} out of {pokemon_df.shape[0]} Pokemon complete')
    
    for key, type_list in problem_pokes.items():
        for typ in type_list:
            if typ not in list(pokemon_df.type_1) or typ not in list(pokemon_df.type_2):
                print (f'ERROR @ : {key}')
        pokemon_df['type_1'].loc[key] = type_list[0]
        if len(type_list) > 1:
            pokemon_df['type_2'].loc[key] = type_list[1]
    

    print ('completed all')
    pokemon_df.to_csv('~/Pokestars/data/clean/pokemon_withtypes_reference.csv')