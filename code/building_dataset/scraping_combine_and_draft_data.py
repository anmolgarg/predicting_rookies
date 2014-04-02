import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns
import random

import requests
from bs4 import BeautifulSoup
import string
import re

def read_in_combine():
	'''Reads in downloaded combine data from http://nflcombineresults.com/.
	Returns a dataframe'''
	names = ['year_combine', 'name', 'college', 'position', 'height', 'weight', 'wonderlic', 
         'forty_yard', 'bench_press', 'vertical_leap', 'broad_jump', 'shuttle', 'three_cone']
	combine = pd.read_csv('raw_data/combine_raw.csv', header=0, names=names)

	# clean up 'forty yard' feature and standardize name
	combine['forty_yard'] = [float(str(d).replace('*','')) if d!=nan else nan for d in combine['forty_yard']]

	combine['first_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[0].strip() 
	                         for d in combine.name]
	combine['last_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[1].strip() 
	                        for d in combine.name]
	combine = combine.drop('name', axis=1)
	
	# reorder columns to put name up front
	cols = combine.columns.tolist()
	cols = cols[-2:] + cols[:-2]
	combine[cols].to_csv('data/combine_data.csv', sep=',')
	return combine[cols]

def get_draft_data():
	'''Scrapes draft data from drafthistory.com for drafts occuring from 2006 to 2014.
	Returns a dataframe'''
	seasons = arange(2006, 2014) # seasons 2006-2013
    draft_bank = pd.DataFrame()
    for season in seasons:
        url = 'http://www.drafthistory.com/index.php/years/'+str(season)
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        for tr in soup.find_all('tr')[3:-9]:        # to go into the table row-wise
            playerstats = []                      
            for td in tr.find_all('td'):            # to go into each column 
                playerstats.append(td.text)         # creates a list of each player's stats
            playerstats.append(season)                                
            draft_bank = pd.concat((draft_bank, pd.DataFrame(playerstats)), axis=1, ignore_index=True)
    print "Number of players in your dataset:", draft_bank.shape[1]
    draft_bank = draft_bank.T.drop(0,axis=1).drop(1,axis=1)
    
    # rename columns and standardize names
    draft_bank.columns = ['pick', 'name', 'team', 'position', 'college', 'year_drafted']
	draft_bank['first_name'] = [string.split(d.encode('ascii','ignore').lower().translate(table, string.punctuation), 
	                                         sep=' ')[0].strip() for d in draft_bank.name]
	draft_bank['last_name'] = [string.split(d.encode('ascii','ignore').lower().translate(table, string.punctuation), 
	                                        sep=' ')[1].strip() for d in draft_bank.name]
	draft_bank = draft_bank.drop('name', axis=1)
	
	# reorder columns to put name up front
	cols = draft_bank.columns.tolist()
	cols = cols[-2:] + cols[:-2]
	draft_bank[cols].to_csv('data/draft_data.csv', sep=',')
	return draft_bank[cols]
