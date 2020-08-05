from bs4 import BeautifulSoup
from collections import defaultdict
import requests
import json

def make_link(head, tail, i):
    """
    Creates the url for all .json logs and 
    INPUT: 
        head - str , https:....
        tail - str , .....json
        i    - int , year
    OUTPUT:
        lst -> url & filepath (fp)
    """       
    folder = '../data/raw/chaos/'
    url = head + str(i).zfill(2) + tail 
    fp = folder + url.split('/chaos')[0][-7:].replace('-', '_') + '.json'
    
    return [url, fp]

def write_data(lst):
    d = get_data(lst[0]) #url is 0th index
    with open(lst[1], 'w') as fp:
        json.dump(d, fp)
        
def get_data(url):
    r = requests.get(url)
    content = r.content
    data = json.loads(content)
    return data

def make_url_list():
#*NOTE** The -0 tag denotes the unweighted usage stats. 
#*It's probably more interesting to look at usage weighted for higher ranked players
#*but the data parsing is cleaner in unweighted statistic and the purpose of using this data 
#*is for SQL analysis so I went with that. 
#* the alternative link would be ou-0 => ou-1500 or ou-1695 
#* The tier could also be changed to look at 'weaker' pokemon, though ou is the most popular and
#* will produce a more robust dataset
#* the alternative link would be ou-0 => nu-0 or uu-0 
#* See smogon.com for more info
    url_list = []
    tail = '/chaos/ou-0.json'
    for i in range(14, 21):
        base_url = f'https://www.smogon.com/stats/20{i}-'
        for j in range(1, 13):
            try:
                url_list.append(make_link(base_url, tail, i))
            except:
                # changes on 17-07, again on 19-12
                if i == 17 and j == 7:
                    tail = '/chaos/gen7ou-0.json'
                elif i == 19 and j == 12:
                    tail = '/chaos/gen8ou-0.json'
                else:
                    continue
    return url_list

if __name__ == '__main__':
    url_list = make_url_list()
    for lst in url_list:
        print (f'Storing {lst[1]}')
        write_data(lst)
    print ('completed')