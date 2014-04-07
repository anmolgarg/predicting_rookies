# Scrapes and preprocesses NFL Fantasy data from http://fftoday.com/

import pandas as pd
import string
import requests
from bs4 import BeautifulSoup

def get_fantasy_data(position):
    '''Scrapes fantasy data for the given position for seasons 2005 to 2014
    Returns a dataframe'''
    positions = {'QB':10, 'RB':20, 'WR':30, 'TE':40, 'DL':50, 'LB':60, 'DB':70, 'K':80}
    pages = arange(0,5)          # top 250 fantasy scorers for the given position/season
    seasons = arange(2005, 2014) # seasons 2005-2013
    position_data = pd.DataFrame()
    
    for season in seasons:
        for page in pages:
            url = 'http://fftoday.com/stats/playerstats.php?Season='+str(season)+'&GameWeek=Season&PosID='+\
                  str(positions[position])+'&LeagueID=1&order_by=FFPts&sort_order=DESC&cur_page='+str(page)
            r = requests.get(url)
            soup = BeautifulSoup(r.content)
            for tr in soup.find_all('tr')[20:-1]:                # to go into the table row-wise
                playerstats = []                      
                for td in tr.find_all('td'):                     # to go into each column 
                    playerstats.append(td.text.replace(',', '')) # creates a list of each player's stats
                playerstats.append(season)
                playerstats.append(position)
                position_data = pd.concat((position_data, pd.DataFrame(playerstats)), axis=1, ignore_index=True)
    position_data = position_data.T
    position_data[0] = [string.replace(s.encode('ascii','ignore'),'. ','',maxreplace=1) for s in position_data[0]]
    position_data[0] = [''.join(i for i in s if not i.isdigit()) for s in position_data[0]]
    print "Number of players in your dataset:", position_data.shape[0]
    return position_data


def make_floats(df):
    cols = set(df.columns)
    cols.remove('name')
    cols.remove('team')
    cols.remove('position')
    df[list(cols)] = df[list(cols)].astype(float)
    return df


def build_full_fantasy_data_set():
    '''Builds full data set for all positions returns a dataframe'''
    
    first = ['name', 'team', 'games_played']
    last =  ['ffpts', 'ffpts_gp', 'nfl_season', 'position']
    #offense
    qb = get_fantasy_data('QB')
    qb.columns = first+['pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int', 
                             'rush_att', 'rush_yd', 'rush_td']+last
    qb = make_floats(qb)

    rb = get_fantasy_data('RB')
    rb.columns = first+['rush_att', 'rush_yd', 'rush_td', 
                        'rec_target', 'rec_reception', 'rec_yd', 'rec_td']+last
    rb = make_floats(rb)
    
    wr = get_fantasy_data('WR')
    wr.columns = first+['rec_target', 'rec_reception', 'rec_yd', 'rec_td',
                        'rush_att', 'rush_yd', 'rush_td']+last
    wr = make_floats(wr)
    
    te = get_fantasy_data('TE')
    te.columns = first+['rec_target', 'rec_reception', 'rec_yd', 'rec_td']+last
    te = make_floats(te)

    ki = get_fantasy_data('K')
    ki.columns = first+['fgm', 'fga', 'fg_perc', 'epm', 'epa']+last
    ki = ki.drop('fg_perc', axis=1)
    ki = make_floats(ki)

    # defense
    defense_cols = first+['tackle', 'assist', 'sack', 'pd', 'int', 'ff', 'fr']+last
    dl = get_fantasy_data('DL')
    dl.columns = defense_cols
    dl = make_floats(dl)
    
    lb = get_fantasy_data('LB')
    lb.columns = defense_cols
    lb = make_floats(lb)
    
    db = get_fantasy_data('DB')
    db.columns = defense_cols
    db = make_floats(db)

    # concatenate datasets
    all_positions = pd.concat((qb, rb, wr, te, ki, dl, lb, db), ignore_index=True)
    
    # standardize names
    table = string.maketrans("","")
    all_positions['first_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[0].strip() 
                              for d in all_positions.name]
    all_positions['last_name'] = [string.split(d.lower().translate(table, string.punctuation), sep=' ')[1].strip() 
                             for d in all_positions.name]
    all_positions = all_positions.drop('name', axis=1)
    
    # reorder columns
    col_order = ['first_name', 'last_name', 'nfl_season', 'position', 'ffpts', 'team', 'games_played', 'ffpts_gp',
     'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int',       # passing
     'rush_att', 'rush_yd', 'rush_td',                                # rushing
     'rec_target', 'rec_reception', 'rec_yd', 'rec_td',               # receiving
     'fgm', 'fga', 'epm', 'epa',                                      # kicking
     'tackle', 'assist', 'sack', 'pd', 'int', 'ff', 'fr']             # defending
    all_positions = all_positions.reindex_axis(col_order, axis=1)
    return all_positions


def build_rookies():
    '''Builds data set for just rookies from 2006 and beyond
    Returns a dataframe'''
    df = build_full_fantasy_data_set()
    
    # group by player name and grab just their first three seasons
    first_season = df.groupby(['last_name', 'first_name']).apply(
        lambda t: t[t.nfl_season==t.nfl_season.min()])
    second_season = df.groupby(['last_name', 'first_name']).apply(
        lambda t: t[t.nfl_season==t.nfl_season.min()+1])
    third_season = df.groupby(['last_name', 'first_name']).apply(
        lambda t: t[t.nfl_season==t.nfl_season.min()+2])
    
    # remove anyone who's first season was in 2005
    rookies = first_season[first_season.nfl_season > 2005]
    rookies = pd.merge(rookies, second_season, how='left', on=['last_name', 'first_name'], suffixes=['', '_second'])
    rookies = pd.merge(rookies, third_season,  how='left', on=['last_name', 'first_name'], suffixes=['', '_third'])
    rookies.to_csv('data/rookies.csv', sep=',')
    return rookies
