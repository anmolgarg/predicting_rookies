import zipfile
import pandas as pd
import string
import requests
from bs4 import BeautifulSoup

import load_ncaa, scrape_combine, scrape_draft, scrape_fantasy

def standardize_college(col):
    table = string.maketrans("","")
    col = col.replace('USC', 'Southern California')
    col = col.replace('Northwest Missouri St', 'Northwest Missouri State')
    col = col.replace('Southeastern Louisiana', 'Southeast Louisiana')
    col = col.replace('Missouri Southern State', 'Missouri Southern')
    col = col.replace('LSU', 'Louisiana State')
    col = col.replace('Tennessee-Chattanooga', 'Chattanooga')
    col = col.replace('BYU', 'Brigham Young')
    col = col.replace('TCU', 'Texas Christian')
    col = col.replace('Stephen F Austin', 'Stephen F. Austin')
    col = col.replace('Middle Tennessee St', 'Middle Tennessee State')
    col = col.replace('Southern Miss', 'Southern Mississippi')
    col = col.replace('Alabama St.', 'Alabama State')
    col = col.replace('SMU', 'Southern Methodist')
    col = col.replace('UAB', 'Alabama Birmingham')
    col = col.replace('UNLV', 'Nevada Las Vegas')
    col = col.replace('UTEP', 'Texas El Paso')
    col = col.replace('UCF', 'Central Florida')

    col = [string.split(str(d).lower(), ' (')[0].strip() for d in col]
    col = [d.translate(table, string.punctuation).strip().replace(' ','') for d in col]
    return col

def standardize_names(col):
    col = col.replace('pat', 'patrick').replace('ziggy', 'ezekial')
    col = col.replace('erick', 'eric').replace('will', 'william')
    col = col.replace('ballmer', 'balmer').replace('josh', 'joshua')
    col = col.replace('johnathan', 'john').replace('herb', 'herbert')
    col = col.replace('mike', 'michael').replace('chris', 'christopher')
    col = col.replace('joe', 'joseph').replace('rob', 'robert')
    col = col.replace('gabe', 'gabriel').replace('nate', 'nathan')
    return col

def merge_combine_and_draft():
    '''Outer joins combine and draft data and returns a dataframe'''
    
    combine = get_combine_data()
    draft = get_draft_data()

    # standardize names of players and colleges
    combine['college'] = standardize_college(combine['college'])
    combine['first_name'] = standardize_names(combine['first_name'])
    combine['last_name'] =  standardize_names(combine['last_name'])
    
    draft['college'] = standardize_college(draft['college'])
    draft['first_name'] = standardize_names(draft['first_name'])
    draft['last_name'] =  standardize_names(draft['last_name'])
    
    # outer join combine and draft
    combine_and_draft = pd.merge(combine, draft, how='outer', suffixes=['_draft', '_combine'], 
                                 on=['last_name', 'first_name', 'college']).sort_index(
                                 by=['last_name', 'first_name'], ascending=True)
    combine_and_draft.reset_index(inplace=True)
    print 'Number of players in merged dataset:', combine_and_draft.shape[0]
    return combine_and_draft


def merge_combine_draft_ncaa_and_fantasy():
    '''Joins all four data sources and returns a dataframe'''
    
    combine_draft = merge_combine_and_draft()
    ncaa = aggregate_stats()
    fantasy = build_rookies()
    
    # standardize college and name
    ncaa['college'] = standardize_college(ncaa['college'])
    ncaa['first_name'] = standardize_names(ncaa['first_name'])
    ncaa['last_name'] =  standardize_names(ncaa['last_name'])
    
    fantasy['first_name'] = standardize_names(fantasy['first_name'])
    fantasy['last_name'] =  standardize_names(fantasy['last_name'])
    
    # outer join combine_draft with ncaa for comprehensive pre-NFL statistics
    df = pd.merge(ncaa, combine_draft, how='outer', suffixes=['_ncaa', '_drcomb'], 
                       on=['last_name', 'first_name', 'college']).sort_index(\
                       by=['last_name', 'first_name'], ascending=True)
    df.rename(columns={'Position':'position'}, inplace=True)
    
    # inner join combine_draft and ncaa with fantasy
    df = pd.merge(fantasy, df, how='inner', suffixes=['_fantasy', '_prepro'], 
         on=['last_name', 'first_name']).sort_index(by=['last_name', 'first_name'], ascending=True)
    # put name back together for convenience
    df['name'] = [str(d[0])+' '+str(d[1]) for d in zip(df.first_name, df.last_name)]
    df = df.drop(['first_name', 'last_name'], axis=1)
    
    print 'Number of players in final dataset:', df.shape[0]
    return df

def clean_up_data():
    '''Builds up dataset, deletes duplicates, and reorders/renames columns
    Returns a dataframe'''
    
    df = merge_combine_draft_ncaa_and_fantasy()

    # deleting duplicate rows from mismatches
    to_delete = [10, 11, 12, 13, 14, 35, 37, 43, 56, 57, 61, 70, 76, 121, 140, 141, 
         144, 156, 194, 221, 223, 225, 227, 228, 229, 230, 232, 237, 238, 240, 
         242, 245, 246, 250, 256, 257, 259, 260, 261, 262, 295, 313, 326, 327, 
         328, 329, 331, 332, 351, 363, 386, 400, 412, 413, 416, 417, 441, 450, 
         451, 459, 461, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 474, 
         478, 480, 481, 482, 484, 485, 487, 488, 490, 494, 496, 498, 499, 501, 
         549, 564, 569, 579, 586, 624, 625, 627, 635, 642, 649, 660, 673, 696, 
         697, 717, 719, 730, 747, 748, 758, 759, 764, 770, 771, 776, 777, 779, 
         783, 795, 807, 828, 830, 835, 837, 839, 840, 842, 846, 867, 870, 876, 
         881, 888, 893, 903, 918, 922, 923, 924, 925, 926, 928, 929, 936, 939, 
         945, 958, 963, 964, 978, 979, 984, 985, 986, 987, 988, 989, 991, 992, 
         993, 994, 995, 996, 1000, 1002, 1003, 1004, 1005, 1009, 1010, 1011, 1013, 
         1014, 1015, 1016, 1017, 1018, 1020, 1021, 1025, 1026, 1028, 1029, 1030, 
         1034, 1037, 1038, 1039, 1040, 1041, 1042, 1046, 1047, 1048, 1049, 1051, 
         1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 
         1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 
         1076, 1077, 1078, 1079, 1080, 1081, 1085, 1086, 1088, 1090, 1091, 1094, 
         1095, 1096, 1097, 1098, 1099, 1100, 1101, 1103, 1109, 1110, 1113, 1114, 
         1115, 1116, 1117, 1118, 1119, 1120, 1122, 1123, 1124, 1125, 1126, 1128, 
         1129, 1131, 1136, 1138, 1140, 1141, 1142, 1144, 1146, 1147, 1148, 1151, 
         1152, 1153, 1155, 1156, 1186, 1188, 1190, 1228, 1229, 1248, 1256, 1258, 
         1259, 1311, 1317, 1321, 1322, 1325, 1338, 1375, 1396, 1420, 1424, 1428, 
         1429, 1434, 1435, 1436, 1437, 1438, 1447, 1452, 1453, 1462, 1464, 1476, 
         1481, 1483, 1488, 1521, 1538, 1552, 1570, 1572, 1573, 1586, 1587, 1592, 
         1609, 1612, 1619, 1632, 1649, 1654, 1657, 1689, 1703, 1705, 1725, 1727, 
         1728, 1731, 1734, 1743, 1745, 1799, 1828, 1838, 1846, 1847, 1849, 1851, 
         1853, 1858, 1860, 1861, 1863, 1866, 1867, 1868, 1869, 1870, 1872, 1873, 
         1876, 1877, 1878, 1879, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1890, 
         1898, 1928, 1930, 1931, 1933, 1970, 1973, 1979, 1980, 1981, 1990, 1992, 
         1994, 1997, 1999, 2003, 2004, 2005, 2007, 2008, 2009, 2010, 2011, 2012, 
         2013, 2015, 2020, 2022, 2023, 2024, 2026, 2028, 2031, 2081, 2084, 2088, 
         2105, 2127, 2129, 2130, 2131, 2132, 2133, 2137, 2139, 2153, 2154, 2155, 
         2156, 2157, 2159, 2160, 2161, 2162, 2163, 2164, 2165, 2166, 2167, 2168, 
         2170, 2171, 2172, 2175, 2178, 2182, 2183, 2185, 2187, 2191, 2192, 2193, 
         2194, 2198, 2200, 2202, 2203, 2204, 2209, 2210, 2211, 2212, 2213, 2214, 
         2216, 2217, 2218, 2219, 2220, 2221, 2222, 2223, 2228, 2229, 2231, 2235, 
         2239, 2240, 2241, 2242, 2244, 2246, 2247, 2248, 2250, 2258, 2259, 2261, 
         2273, 2278, 2285]
    df = df.drop(to_delete)

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
        
        'Pass Att_sum', 'Pass Comp_sum', 'Pass Conv_sum', 'Pass Int_sum', 'Pass TD_sum', 'Pass Yard_sum', 
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
        'Kickoff Onside_sum', 'Kickoff Out-Of-Bounds_sum',
        
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
    print 'Number of clean players in final dataset:', df.shape[0]
    return df