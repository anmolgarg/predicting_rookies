# Loads and preprocesses NCAA data from http://www.cfbstats.com/

import zipfile
import pandas as pd
import string

def build_team_alignment():
    '''Reads in teams and conferences datasets for current alignment and joins'''
    archive = zipfile.ZipFile('data/cfbstats.com-2013-1.5.20.zip', 'r')
    teams = pd.read_csv(archive.extract('team.csv'), header=0, names=['Team Code', 'college', 'conf_code'])
    confs = pd.read_csv(archive.extract('conference.csv'), header=0, names=['conf_code', 'conference', 'subn'])
    teams_plus = pd.merge(teams, confs)
    return teams_plus.drop('conf_code', axis=1)


def build_stats_and_players():
    '''Reads in NCAA player stats and game stats and concatenates seasons 2005-2013.
    Returns a players dataframe and a stats dataframe'''
    
    table = string.maketrans("","")
    
    all_players = pd.DataFrame()
    all_stats = pd.DataFrame()
    # goes through all years of data and concatenates seasons together row-wise for players and games
    for yearspan in ['2013-1.5.20', '2012-1.5.4', '2011-1.5.0', '2010-1.5.0',
        '2009-1.5.0', '2008-1.5.0', '2007-1.5.0', '2006-1.5.0', '2005-1.5.0']:
        archive = zipfile.ZipFile('data/cfbstats.com-'+str(yearspan)+'.zip', 'r')
        players = pd.read_csv(archive.extract('player.csv'), header=0)
        players['ncaa_season'] = string.split(yearspan, '-')[0]
        print yearspan, players.shape
        all_players = pd.concat((all_players, players), ignore_index=True)
        
        player_stats = pd.read_csv(archive.extract('player-game-statistics.csv'), header=0)
        player_stats['ncaa_season'] = string.split(yearspan, '-')[0]
        print yearspan, player_stats.shape
        all_stats = pd.concat((all_stats, player_stats), ignore_index=True)
    
    # standardizes names    
    all_players['first_name'] = [str(d).lower().translate(table, string.punctuation).strip()\
    for d in all_players['First Name']]
    all_players['last_name'] = [str(d).lower().translate(table, string.punctuation).strip()\
    for d in all_players['Last Name']]
    # rearrange columns to put standardized name and season to front of dataframe
    cols = all_players.columns.tolist()
    cols = cols[-3:] + cols[:-3]
    all_players = all_players[cols].drop('Last Name', axis=1).drop('First Name', axis=1)
    return all_players, all_stats


def aggregate_stats():
    '''Builds out NCAA teams, players, and stats dataframes
    Groups players and game stats dataframes by player code and takes only their final season
    Returns a dataframe aggregated by player, and writes df to csv'''
    
    # build team alignment, players, and games dataframes
    teams_plus = build_team_alignment()
    all_players, all_stats = build_stats_and_players()
    print 'built dataframes'
    
    #  group players by player selecting only final season
    all_players_by = all_players.groupby(['Player Code']).apply(lambda t: t[t.ncaa_season==t.ncaa_season.max()])
    all_players_by['Player Code'] = [d[0] for d in all_players_by.index]
    print 'grouped players'
    
    # aggregating stats using sums; groups by player and grabs all rows for their max ncaa season, then aggregates
    all_stats_by = all_stats.groupby(['Player Code']).apply(lambda t: t[t.ncaa_season==t.ncaa_season.max()])
    all_stats_sum = all_stats_by.groupby(['Player Code', 'ncaa_season']).sum()
    all_stats_sum['Player Code'] = [d[0] for d in all_stats_sum.index]
    all_stats_sum['ncaa_season'] = [d[1] for d in all_stats_sum.index]
    
    # stats aggregating using means
    all_stats_mean = all_stats_by.groupby(['Player Code', 'ncaa_season']).mean()
    all_stats_mean['Player Code'] = [d[0] for d in all_stats_mean.index]
    all_stats_mean['ncaa_season'] = [d[1] for d in all_stats_mean.index]
    print 'aggregated stats'
    
    # merge sum and mean stats and then merge with players
    all_stats = pd.merge(all_stats_sum, all_stats_mean, on=['Player Code', 'ncaa_season'], 
                         suffixes=['_sum', '_mean'])
    all_ncaa = pd.merge(all_players_by, all_stats, how='right', on='Player Code', suffixes=['_player', '_stats'])
    all_ncaa = pd.merge(all_ncaa, teams_plus, on='Team Code', how='left')
    
    # reorder columns
    col_order = ['ncaa_season_player', 'first_name', 'last_name', 'Class', 'Height', 'Weight', 
     'Home Country', 'Home State', 'Home Town', 'Last School', 'Player Code', 
     'Position', 'Team Code', 'Uniform Number', 'college', 'conference', 'subn',
     
     'ncaa_season_stats', 'Def 2XP Att_sum', 'Def 2XP Made_sum', 'Field Goal Att_sum', 'Field Goal Made_sum', 
     'Fum Ret_sum', 'Fum Ret TD_sum', 'Fum Ret Yard_sum', 
     'Fumble_sum', 'Fumble Forced_sum', 'Fumble Lost_sum', 'Game Code_sum', 'Int Ret_sum', 'Int Ret TD_sum', 
     'Int Ret Yard_sum', 'Kick/Punt Blocked_sum', 'Kickoff_sum', 'Kickoff Onside_sum', 
     'Kickoff Out-Of-Bounds_sum', 'Kickoff Ret_sum', 'Kickoff Ret TD_sum', 'Kickoff Ret Yard_sum', 
     'Kickoff Touchback_sum', 'Kickoff Yard_sum', 'Misc Ret_sum', 'Misc Ret TD_sum', 'Misc Ret Yard_sum', 
     'Off 2XP Att_sum', 'Off 2XP Made_sum', 'Off XP Kick Att_sum', 'Off XP Kick Made_sum', 'Pass Att_sum', 
     'Pass Broken Up_sum', 'Pass Comp_sum', 'Pass Conv_sum', 'Pass Int_sum', 'Pass TD_sum', 'Pass Yard_sum', 
     'Points_sum', 'Punt_sum', 'Punt Ret_sum', 'Punt Ret TD_sum', 'Punt Ret Yard_sum', 'Punt Yard_sum', 
     'QB Hurry_sum', 'Rec_sum', 'Rec TD_sum', 'Rec Yards_sum', 'Rush Att_sum', 'Rush TD_sum', 'Rush Yard_sum', 
     'Sack_sum', 'Sack Yard_sum', 'Safety_sum', 'Tackle Assist_sum', 'Tackle For Loss_sum', 'Tackle For Loss Yard_sum', 
     'Tackle Solo_sum', 
     
     'Def 2XP Att_mean', 'Def 2XP Made_mean', 'Field Goal Att_mean', 'Field Goal Made_mean', 
     'Fum Ret_mean', 'Fum Ret TD_mean', 'Fum Ret Yard_mean', 'Fumble_mean', 
     'Fumble Forced_mean', 'Fumble Lost_mean', 'Game Code_mean', 'Int Ret_mean', 'Int Ret TD_mean', 
     'Int Ret Yard_mean', 'Kick/Punt Blocked_mean', 'Kickoff_mean', 'Kickoff Onside_mean', 'Kickoff Out-Of-Bounds_mean', 
     'Kickoff Ret_mean', 'Kickoff Ret TD_mean', 'Kickoff Ret Yard_mean', 'Kickoff Touchback_mean', 
     'Kickoff Yard_mean', 'Misc Ret_mean', 'Misc Ret TD_mean', 'Misc Ret Yard_mean', 'Off 2XP Att_mean', 
     'Off 2XP Made_mean', 'Off XP Kick Att_mean', 'Off XP Kick Made_mean', 'Pass Att_mean', 'Pass Broken Up_mean', 
     'Pass Comp_mean', 'Pass Conv_mean', 'Pass Int_mean', 'Pass TD_mean', 'Pass Yard_mean', 'Points_mean', 
     'Punt_mean', 'Punt Ret_mean', 'Punt Ret TD_mean', 'Punt Ret Yard_mean', 'Punt Yard_mean', 'QB Hurry_mean', 
     'Rec_mean', 'Rec TD_mean', 'Rec Yards_mean', 'Rush Att_mean', 'Rush TD_mean', 'Rush Yard_mean', 'Sack_mean', 
     'Sack Yard_mean', 'Safety_mean', 'Tackle Assist_mean', 'Tackle For Loss_mean', 'Tackle For Loss Yard_mean', 
     'Tackle Solo_mean']
    all_ncaa = all_ncaa.reindex_axis(col_order, axis=1)
    all_ncaa.to_csv('data/ncaa.csv', sep=',')
    print 'wrote to csv'
    
    return all_ncaa