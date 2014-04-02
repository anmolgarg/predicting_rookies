import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns
import random

import requests
from bs4 import BeautifulSoup
import string
import re

def get_fantasy_data(position):
	'''Scrapes fantasy data from fftoday.com for the given posiiton for seasons 2005 to 2014.
	Returns a dataframe'''
    positions = {'QB':10, 'RB':20, 'WR':30, 'TE':40, 'DL':50, 'LB':60, 'DB':70, 'K':80}
    pages = arange(0,5)          # top 250 fantasy scorers for the given position/season
	seasons = arange(2005, 2014) # seasons 2005-2013
    pos_bank = pd.DataFrame()

    for season in seasons:
        for page in pages:
            url = 'http://fftoday.com/stats/playerstats.php?Season='+str(season)+'&GameWeek=Season&PosID='+\
                  str(positions[position])+'&LeagueID=1&order_by=FFPts&sort_order=DESC&cur_page='+str(page)
            r = requests.get(url)
            soup = BeautifulSoup(r.content)
            for tr in soup.find_all('tr')[20:-1]:                        # to go into the table row-wise
                playerstats = []                      
                for td in tr.find_all('td'):                             # to go into each column 
                    playerstats.append(td.text.replace(',', ''))         # creates a list of each player's stats
                playerstats.append(season)
                playerstats.append(position)
                pos_bank = pd.concat((pos_bank, pd.DataFrame(playerstats)), axis=1, ignore_index=True)
    pos_bank.T[0] = [string.replace(s.encode('ascii','ignore'),'. ','',maxreplace=1) for s in pos_bank.T[0]]
    pos_bank.T[0] = [''.join(i for i in s if not i.isdigit()) for s in pos_bank.T[0]]
    print 'Number of players in df:', pos_bank.T.shape[0]
    return pos_bank.T

def floatify(bank):
    cols = set(bank.columns)
    cols.remove('name')
    cols.remove('team')
    cols.remove('position')
    bank[list(cols)] = bank[list(cols)].astype(float)
    return bank

def build_data_set():
	'''Builds full fantasy data set for all positions using get_fantasy_data and floatify.
	Returns a dataframe'''
	#offense
	qb_bank = get_fantasy_data('QB')
	qb_bank.columns = ['name', 'team', 'games_played', 'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int', 
	                   'rush_att', 'rush_yd', 'rush_td', 'ffpts', 'ffpts_gp', 'nfl_season', 'position']
	qb_bank = floatify(qb_bank)

	rb_bank = get_fantasy_data('RB')
	rb_bank.columns = ['name', 'team', 'games_played', 'rush_att', 'rush_yd', 'rush_td', 'rec_target', 
	                   'rec_reception', 'rec_yd', 'rec_td', 'ffpts', 'ffpts_gp', 'nfl_season', 'position']
	rb_bank = floatify(rb_bank)

	reccols = ['name', 'team', 'games_played', 'rec_target', 'rec_reception', 'rec_yd', 'rec_td', 
	                   'ffpts', 'ffpts_gp', 'nfl_season', 'position']
	wr_bank = get_fantasy_data('WR')
	wr_bank.columns = reccols
	wr_bank = floatify(wr_bank)
	te_bank = get_fantasy_data('TE')
	te_bank.columns = reccols
	te_bank = floatify(te_bank)

	ki_bank = get_fantasy_data('K')
	ki_bank.columns = ['name', 'team', 'games_played', 'fgm', 'fga', 'fg_perc', 'epm', 'epa', 
	                   'ffpts', 'ffpts_gp', 'nfl_season', 'position']
	ki_bank = ki_bank.drop('fg_perc', axis=1)
	ki_bank = floatify(ki_bank)

	# defense
	defcols = ['name', 'team', 'games_played', 'tackle', 'assist', 'sack', 'pd', 'int',
	                   'ff', 'fr', 'ffpts', 'ffpts_gp', 'nfl_season', 'position']
	dl_bank = get_fantasy_data('DL')
	dl_bank.columns = defcols
	dl_bank = floatify(dl_bank)
	lb_bank = get_fantasy_data('LB')
	lb_bank.columns = defcols
	lb_bank = floatify(lb_bank)
	db_bank = get_fantasy_data('DB')
	db_bank.columns = defcols
	db_bank = floatify(db_bank)

	# concatenate datasets
	all_bank = pd.concat((qb_bank, rb_bank, wr_bank, te_bank, ki_bank, 
		dl_bank, lb_bank, db_bank), ignore_index=True)
	# standardize names
	table = string.maketrans("","")
	all_bank['first_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[0].strip() 
	                          for d in all_bank.name]
	all_bank['last_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[1].strip() 
	                         for d in all_bank.name]
	all_bank = all_bank.drop('name', axis=1)
	# reorder columns
	col_order = ['first_name', 'last_name', 'nfl_season', 'position', 'ffpts', 'team', 'games_played', 'ffpts_gp',
	 'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int',       # passing
	 'rush_att', 'rush_yd', 'rush_td',                                # rushing
	 'rec_target', 'rec_reception', 'rec_yd', 'rec_td',               # receiving
	 'fgm', 'fga', 'epm', 'epa',                                      # kicking
	 'tackle', 'assist', 'sack', 'pd', 'int', 'ff', 'fr']             # defending
	return all_bank.reindex_axis(col_order, axis=1)

def build_rookies():
	'''Builds data set for just rookies from 2006 and beyond.
	Returns a dataframe'''
	df = build_data_set()
	# group by player name and grab just their first, second, third season
	first_season = df.groupby(['last_name', 'first_name']).apply(
		lambda t: t[t.nfl_season==t.nfl_season.min()])
	second_season = df.groupby(['last_name', 'first_name']).apply(
		lambda t: t[t.nfl_season==t.nfl_season.min()+1])
	third_season = df.groupby(['last_name', 'first_name']).apply(
		lambda t: t[t.nfl_season==t.nfl_season.min()+2])
	# remove anyone who's first season was in 2005 as NCAA stats only go back to 2005
	rookies = first_season[first_season.nfl_season > 2005]
	rookies_plus = pd.merge(rookies, second_season, how='left', on=['last_name', 'first_name'], suffixes=['', '_second'])
	rookies_plus = pd.merge(rookies_plus, third_season, how='left', on=['last_name', 'first_name'], suffixes=['', '_third'])
	rookies_plus.to_csv('data/rookies_df.csv', sep=',')
	return rookies_plus