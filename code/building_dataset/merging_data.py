import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns
import random
import string

combine = pd.read_csv('data/combine_data.csv', header=0, index_col=0)
draft = pd.read_csv('data/draft_data.csv', header=0, index_col=0)
ncaa = pd.read_csv('data/ncaa.csv', sep=',', index_col=0)
fantasy = pd.read_csv('data/all_ff_data.csv', header=0, index_col=0)

def standardize_college(col):
	table = string.maketrans("","")
    col = col.replace('USC', 'Southern California').replace('Northwest Missouri St', 'Northwest Missouri State')
    col = col.replace('Southeastern Louisiana', 'Southeast Louisiana').replace('Missouri Southern State', 'Missouri Southern')
    col = col.replace('LSU', 'Louisiana State').replace('Tennessee-Chattanooga', 'Chattanooga')
    col = col.replace('BYU', 'Brigham Young').replace('TCU', 'Texas Christian').replace('Stephen F Austin', 'Stephen F. Austin')
    col = col.replace('Middle Tennessee St', 'Middle Tennessee State').replace('Southern Miss', 'Southern Mississippi')
    col = col.replace('Alabama St.', 'Alabama State')
    col = [string.split(str(d).lower(), ' (')[0].strip() for d in col]
    col = [d.translate(table, string.punctuation).strip().replace(' ','') for d in col]
    return col

def standardize_names(col):
    col = col.replace('pat', 'patrick').replace('ziggy', 'ezekial').replace('erick', 'eric').replace('will', 'william')
    col = col.replace('ballmer', 'balmer').replace('josh', 'joshua').replace('johnathan', 'john')
    col = col.replace('mike', 'michael').replace('chris', 'christopher').replace('joe', 'joseph').replace('rob', 'robert')
    col = col.replace('gabe', 'gabriel').replace('nate', 'nathan').replace('herb', 'herbert')
    return col

def merge_combine_draft(combine, draft):
	'''Standardizes names of players and colleges and then merges combine and draft data.
	Returns a dataframe.'''
	combine['college'] = standardize_college(combine['college'])
	draft['college'] =   standardize_college(draft['college'])
	combine['first_name'] = standardize_names(combine['first_name'])
	draft['first_name'] =   standardize_names(draft['first_name'])
	combine['last_name'] =  standardize_names(combine['last_name'])
	draft['last_name'] =    standardize_names(draft['last_name'])

	draft_and_combine = pd.merge(draft, combine, how='outer', suffixes=['_drafted', '_combine'], 
	                             on=['last_name', 'first_name', 'college']).sort_index(
	                             by=['last_name', 'first_name'], ascending=True)
	return draft_and_combine

def merge_combine_draft_ncaa_fantasy(combine, draft, ncaa, fantasy):
	'''Standardizes names of players and colleges and then merges all 4 data sources.
	Returns a dataframe.'''	
	draft_and_combine = merge_combine_draft(combine, draft)
	ncaa['college'] = standardize_college(ncaa['college'])
	ncaa['first_name'] = standardize_names(ncaa['first_name'])
	ncaa['last_name'] =  standardize_names(ncaa['last_name'])
	pre_pro = pd.merge(ncaa, draft_and_combine, how='outer', suffixes=['_ncaa', '_drcomb'], 
	                   on=['last_name', 'first_name', 'college']).sort_index(
	                   by=['last_name', 'first_name'], ascending=True)
	pre_pro.rename(columns={'Position':'position'}, inplace=True)

	fantasy['first_name'] = standardize_names(fantasy['first_name'])
	fantasy['last_name'] =  standardize_names(fantasy['last_name'])
	df = pd.merge(fantasy, pre_pro, how='inner', suffixes=['_fantasy', '_prepro'], 
         on=['last_name', 'first_name']).sort_index(by=['last_name', 'first_name'], ascending=True)
	df['name'] = [str(d[0])+' '+str(d[1]) for d in zip(df.first_name, df.last_name)]
	df = df.drop(['first_name', 'last_name'], axis=1)
	# selecting columns
	cols = ['name', 
        'nfl_season', 'position_fantasy', 'ffpts', 'ffpts_gp', 'team_fantasy', 'games_played',
        'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int', 
        'rush_att', 'rush_yd', 'rush_td', 
        'rec_target', 'rec_reception', 'rec_yd', 'rec_td', 
        'fgm', 'fga', 'epm', 'epa', 'tackle', 'assist', 
        'sack', 'pd', 'int', 'ff', 'fr', 
        
        'nfl_season_second', 'position_second', 'ffpts_second', 'team_second', 'games_played_second', 'ffpts_gp_second', 
        'pass_comp_second', 'pass_att_second', 'pass_yd_second', 'pass_td_second', 'pass_int_second', 
        'rush_att_second', 'rush_yd_second', 'rush_td_second', 
        'rec_target_second', 'rec_reception_second', 'rec_yd_second', 'rec_td_second', 
        'fgm_second', 'fga_second', 'epm_second', 'epa_second', 'tackle_second', 'assist_second', 
        'sack_second', 'pd_second', 'int_second', 'ff_second', 'fr_second', 
        
        'nfl_season_third', 'position_third', 'ffpts_third', 'team_third', 'games_played_third', 'ffpts_gp_third', 
        'pass_comp_third', 'pass_att_third', 'pass_yd_third', 'pass_td_third', 'pass_int_third', 
        'rush_att_third', 'rush_yd_third', 'rush_td_third', 
        'rec_target_third', 'rec_reception_third', 'rec_yd_third', 'rec_td_third', 
        'fgm_third', 'fga_third', 'epm_third', 'epa_third', 'tackle_third', 'assist_third', 
        'sack_third', 'pd_third', 'int_third', 'ff_third', 'fr_third', 
        
        'pick', 'height', 'weight', 'wonderlic', 'forty_yard', 'bench_press', 'vertical_leap', 'broad_jump', 
        'shuttle', 'three_cone', 
        
        'Pass Att', 'Pass Comp', 'Pass Conv', 'Pass Int', 'Pass TD', 'Pass Yard', 'QB Hurry', 'Rec', 'Rec TD', 
        'Rec Yards', 'Rush Att', 'Rush TD', 'Rush Yard', 'Fumble', 'Fumble Lost', 'Kickoff Ret', 'Kickoff Ret TD', 
        'Kickoff Ret Yard', 'Kickoff Touchback', 'Punt Ret', 'Punt Ret TD', 'Punt Ret Yard', 'Off 2XP Att', 
        'Off 2XP Made', 'Points', 'Tackle Assist', 'Tackle For Loss', 'Tackle For Loss Yard', 'Tackle Solo', 
        'Pass Broken Up', 'Sack', 'Sack Yard', 'Int Ret', 'Int Ret TD', 'Int Ret Yard', 'Misc Ret', 'Misc Ret TD', 
        'Misc Ret Yard', 'Kick/Punt Blocked', 'Fumble Forced', 'Fum Ret', 'Fum Ret TD', 'Fum Ret Yard', 'Def 2XP Att', 
        'Def 2XP Made', 'Safety', 'Field Goal Att', 'Field Goal Made', 'Off XP Kick Att', 'Off XP Kick Made', 
        'Punt', 'Punt Yard', 'Kickoff', 'Kickoff Yard', 'Kickoff Onside', 'Kickoff Out-Of-Bounds',
        
        'Pass Att_mean', 'Pass Comp_mean', 'Pass Conv_mean', 'Pass Int_mean', 'Pass TD_mean', 'Pass Yard_mean', 
        'QB Hurry_mean', 'Rec_mean', 'Rec TD_mean', 'Rec Yards_mean', 
        'Rush Att_mean', 'Rush TD_mean', 'Rush Yard_mean', 'Fumble_mean', 'Fumble Lost_mean', 
        'Kickoff Ret_mean', 'Kickoff Ret TD_mean', 'Kickoff Ret Yard_mean', 'Kickoff Touchback_mean', 
        'Punt Ret_mean', 'Punt Ret TD_mean', 'Punt Ret Yard_mean', 'Off 2XP Att_mean', 'Off 2XP Made_mean', 
        'Points_mean', 'Tackle Assist_mean', 'Tackle For Loss_mean', 'Tackle For Loss Yard_mean', 'Tackle Solo_mean', 
        'Pass Broken Up_mean', 'Sack_mean', 'Sack Yard_mean', 
        'Int Ret_mean', 'Int Ret TD_mean', 'Int Ret Yard_mean', 
        'Misc Ret_mean', 'Misc Ret TD_mean', 'Misc Ret Yard_mean', 'Kick/Punt Blocked_mean', 
        'Fumble Forced_mean', 'Fum Ret_mean', 'Fum Ret TD_mean', 'Fum Ret Yard_mean', 
        'Def 2XP Att_mean', 'Def 2XP Made_mean', 'Safety_mean', 
        'Field Goal Att_mean', 'Field Goal Made_mean', 'Off XP Kick Att_mean', 'Off XP Kick Made_mean', 
        'Punt_mean', 'Punt Yard_mean', 'Kickoff_mean', 'Kickoff Yard_mean', 
        'Kickoff Onside_mean', 'Kickoff Out-Of-Bounds_mean',
        
        'Class', 'college', 'conference', 'subn', 
        'Height', 'Weight', 'Home Country', 'Home State', 'Home Town', 'Last School',]
	df = df[cols]
	
	# reordering columns
	# fantasy
	rookie = ['nfl_season', 'position', 'ffpts', 'ffpts_gp', 'team', 'games_played',
	        'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int', 
	        'rush_att', 'rush_yd', 'rush_td', 
	        'rec_target', 'rec_reception', 'rec_yd', 'rec_td', 
	        'fgm', 'fga', 'epm', 'epa', 'tackle', 'assist', 
	        'sack', 'pd', 'int', 'ff', 'fr']  
	second = ['nfl_season_second', 'position_second', 'ffpts_second', 'team_second', 'games_played_second', 'ffpts_gp_second', 
	        'pass_comp_second', 'pass_att_second', 'pass_yd_second', 'pass_td_second', 'pass_int_second', 
	        'rush_att_second', 'rush_yd_second', 'rush_td_second', 
	        'rec_target_second', 'rec_reception_second', 'rec_yd_second', 'rec_td_second', 
	        'fgm_second', 'fga_second', 'epm_second', 'epa_second', 'tackle_second', 'assist_second', 
	        'sack_second', 'pd_second', 'int_second', 'ff_second', 'fr_second'] 
	third = ['nfl_season_third', 'position_third', 'ffpts_third', 'team_third', 'games_played_third', 'ffpts_gp_third', 
	        'pass_comp_third', 'pass_att_third', 'pass_yd_third', 'pass_td_third', 'pass_int_third', 
	        'rush_att_third', 'rush_yd_third', 'rush_td_third', 
	        'rec_target_third', 'rec_reception_third', 'rec_yd_third', 'rec_td_third', 
	        'fgm_third', 'fga_third', 'epm_third', 'epa_third', 'tackle_third', 'assist_third', 
	        'sack_third', 'pd_third', 'int_third', 'ff_third', 'fr_third']
	# draft and combine
	draft_combine = ['draft_pick', 'height_combine', 'weight_combine', 'wonderlic', 'forty_yard', 
	        'bench_press', 'vertical_leap', 'broad_jump', 'shuttle', 'three_cone']
	# ncaa
	ncaa_sum = ['Pass Att_sum', 'Pass Comp_sum', 'Pass Conv_sum', 'Pass Int_sum', 'Pass TD_sum', 'Pass Yard_sum', 
	        'QB Hurry_sum', 'Rec_sum', 'Rec TD_sum', 'Rec Yards_sum', 
	        'Rush Att_sum', 'Rush TD_sum', 'Rush Yard_sum', 'Fumble_sum', 'Fumble Lost_sum', 
	        'Kickoff Ret_sum', 'Kickoff Ret TD_sum', 'Kickoff Ret Yard_sum', 'Kickoff Touchback_sum', 
	        'Punt Ret_sum', 'Punt Ret TD_sum', 'Punt Ret Yard_sum', 'Off 2XP Att_sum', 'Off 2XP Made_sum', 
	        'Points_sum', 'Tackle Assist_sum', 'Tackle For Loss_sum', 'Tackle For Loss Yard_sum', 'Tackle Solo_sum', 
	        'Pass Broken Up_sum', 'Sack_sum', 'Sack Yard_sum', 
	        'Int Ret_sum', 'Int Ret TD_sum', 'Int Ret Yard_sum', 
	        'Misc Ret_sum', 'Misc Ret TD_sum', 'Misc Ret Yard_sum', 'Kick/Punt Blocked_sum', 
	        'Fumble Forced_sum', 'Fum Ret_sum', 'Fum Ret TD_sum', 'Fum Ret Yard_sum', 
	        'Def 2XP Att_sum', 'Def 2XP Made_sum', 'Safety_sum', 
	        'Field Goal Att_sum', 'Field Goal Made_sum', 'Off XP Kick Att_sum', 'Off XP Kick Made_sum', 
	        'Punt_sum', 'Punt Yard_sum', 'Kickoff_sum', 'Kickoff Yard_sum', 
	        'Kickoff Onside_sum', 'Kickoff Out-Of-Bounds_sum']
	ncaa_mean = ['Pass Att_mean', 'Pass Comp_mean', 'Pass Conv_mean', 'Pass Int_mean', 'Pass TD_mean', 'Pass Yard_mean', 
	        'QB Hurry_mean', 'Rec_mean', 'Rec TD_mean', 'Rec Yards_mean', 
	        'Rush Att_mean', 'Rush TD_mean', 'Rush Yard_mean', 'Fumble_mean', 'Fumble Lost_mean', 
	        'Kickoff Ret_mean', 'Kickoff Ret TD_mean', 'Kickoff Ret Yard_mean', 'Kickoff Touchback_mean', 
	        'Punt Ret_mean', 'Punt Ret TD_mean', 'Punt Ret Yard_mean', 'Off 2XP Att_mean', 'Off 2XP Made_mean', 
	        'Points_mean', 'Tackle Assist_mean', 'Tackle For Loss_mean', 'Tackle For Loss Yard_mean', 'Tackle Solo_mean', 
	        'Pass Broken Up_mean', 'Sack_mean', 'Sack Yard_mean', 
	        'Int Ret_mean', 'Int Ret TD_mean', 'Int Ret Yard_mean', 
	        'Misc Ret_mean', 'Misc Ret TD_mean', 'Misc Ret Yard_mean', 'Kick/Punt Blocked_mean', 
	        'Fumble Forced_mean', 'Fum Ret_mean', 'Fum Ret TD_mean', 'Fum Ret Yard_mean', 
	        'Def 2XP Att_mean', 'Def 2XP Made_mean', 'Safety_mean', 
	        'Field Goal Att_mean', 'Field Goal Made_mean', 'Off XP Kick Att_mean', 'Off XP Kick Made_mean', 
	        'Punt_mean', 'Punt Yard_mean', 'Kickoff_mean', 'Kickoff Yard_mean', 
	        'Kickoff Onside_mean', 'Kickoff Out-Of-Bounds_mean']
	basics = ['class', 'college', 'conference', 'subn', 
	        'height_ncaa', 'weight_ncaa', 'Home Country', 'Home State', 'Home Town', 'Last School',]

	colnames = [['name']+rookie+second+third+draft_combine+ncaa_sum+ncaa_mean+basics]
	df.columns = colnames
	df.to_csv('data/all_data.csv', sep=',')
	return df
