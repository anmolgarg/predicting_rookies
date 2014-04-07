# Scrapes and preprocesses NFL Combine data from http://nflcombineresults.com/

import pandas as pd
import string
import requests
from bs4 import BeautifulSoup

def get_combine_data():
    '''Scrapes combine data for all years and returns a dataframe'''
    combine = pd.DataFrame()
    url = 'http://nflcombineresults.com/nflcombinedata.php?year=all&pos=&college='
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    for tr in soup.find_all('tr')[1:-2]:        # to go into the table row-wise
        playerstats = []                      
        for td in tr.find_all('td'):            # to go into each column 
            playerstats.append(td.text)         # creates a list of each player's stats
        combine = pd.concat((combine, pd.DataFrame(playerstats)), axis=1, ignore_index=True)
    combine = combine.T
    
    # rename columns and standardize names
    combine.columns = ['year_combine', 'name', 'college', 'position', 'height', 'weight', 'wonderlic', 
                       'forty_yard', 'bench_press', 'vertical_leap', 'broad_jump', 'shuttle', 'three_cone']
    # clean up 'forty yard' feature and standardize name
    table = string.maketrans("","")
    combine['forty_yard'] = [float(str(d).replace('*','')) if d!='' else '' for d in combine['forty_yard']]
    combine['first_name'] = [string.split(str(d.lower()).translate(table, string.punctuation), sep=' ')[0].strip() 
                             for d in combine.name]
    combine['last_name'] = [string.split(str(d.lower()).translate(table, string.punctuation), sep=' ')[1].strip() 
                            for d in combine.name]
    combine = combine.drop('name', axis=1)
    
    # drop combiners from 2005 or earlier
    combine_year = combine.year_combine.astype(float)
    ds = []
    for d in combine.index:
        if combine_year[d]<2006:
            ds.append(d)
    combine = combine.drop(ds)
    combine.reset_index(inplace=True)
    print "Number of players in your dataset:", combine.shape[0]

    # reorder columns to put name up front, write to csv
    cols = combine.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    combine = combine[cols]
    combine.to_csv('data/combine.csv', sep=',')
    return combine
