# Scrapes and preprocesses NFL Draft data from http://www.drafthistory.com/

import pandas as pd
import string
import requests
from bs4 import BeautifulSoup

def get_draft_data():
    '''Scrapes all draft history from 2006-2013 and returns a dataframe'''
    seasons = arange(2006, 2014) # seasons 2006-2013
    draft = pd.DataFrame()
    for season in seasons:
        url = 'http://www.drafthistory.com/index.php/years/'+str(season)
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        for tr in soup.find_all('tr')[3:-9]:        # to go into the table row-wise
            playerstats = []                      
            for td in tr.find_all('td'):            # to go into each column 
                playerstats.append(td.text)         # creates a list of each player's stats
            playerstats.append(season)                                
            draft = pd.concat((draft, pd.DataFrame(playerstats)), axis=1, ignore_index=True)
    print "Number of players in your dataset:", draft.shape[1]
    draft = draft.T.drop(0,axis=1).drop(1,axis=1)
    
    # rename columns and standardize names
    table = string.maketrans("","")
    draft.columns = ['pick', 'name', 'team', 'position', 'college', 'year_drafted']
    draft['first_name'] = [string.split(d.encode('ascii','ignore').lower().translate(table, string.punctuation),
                                        sep=' ')[0].strip() for d in draft.name]
    draft['last_name'] = [string.split(d.encode('ascii','ignore').lower().translate(table, string.punctuation), 
                                       sep=' ')[1].strip() for d in draft.name]
    draft = draft.drop('name', axis=1)
    
    # reorder columns to put name up front, write to csv
    cols = draft.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    draft = draft[cols]
    draft.to_csv('data/draft.csv', sep=',')
    return draft
