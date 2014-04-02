import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns
import random
import time

import string
from sklearn import preprocessing
from sklearn.cluster import Ward, KMeans

from sklearn import cross_validation
from sklearn.pipeline import Pipeline
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import confusion_matrix, roc_curve, auc

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

def cluster_players(df):
	'''Performs k-means k=2 clustering of fantasy players.
	Returns updated dataframe with cluster IDs.'''
	kmeans = KMeans(n_clusters=2).fit(df[['ffpts', 'ffpts_gp']])
	labels_2 = kmeans.labels_
	for lab in unique(labels_2):
	    print lab, len(labels_2[labels_2==lab])*1.0/len(labels_2)
	sns.boxplot(df['ffpts'], groupby=labels_2)
	plt.savefig('cluster_results.png')
	df['kmeans_k2'] = labels_2
	return df

# fantasy
rookie = ['nfl_season', 'position', 'ffpts', 'ffpts_gp', 'team', 'games_played',
        'pass_comp', 'pass_att', 'pass_yd', 'pass_td', 'pass_int', 
        'rush_att', 'rush_yd', 'rush_td', 
        'rec_target', 'rec_reception', 'rec_yd', 'rec_td', 
        'fgm', 'fga', 'epm', 'epa', 'tackle', 'assist', 
        'sack', 'pd', 'int', 'ff', 'fr']

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

def map_features(df):
	'''Maps out categoricals (position, college, conference, etc.) to numerics for use in modeling.
	Also forces positive values for the rare negative fantasy points. Returns an updated dataframe.'''
	df = df.replace(0, nan).replace(-1, nan)
	columns = df.columns

	features = pd.DataFrame()
	for col in columns:
	    if df[col].dtype == np.dtype('object'):
	        s = np.unique(df2[col].fillna(-1).values)
	        mapping = pd.Series([x[0] for x in enumerate(s)], index = s)
	        features = pd.concat((features, pd.DataFrame(df2[col].map(mapping).fillna(-1))), axis=1)
	    else:
	        features = pd.concat((features, pd.DataFrame(df2[col].fillna(-1))), axis=1)

	df[['class', 'college', 'conference', 'subn', 'Home Country', 'Home State', 'Home Town', 
     	'Last School', 'position']] = \
	features[['class', 'college', 'conference', 'subn', 'Home Country', 'Home State', 'Home Town', 
        'Last School', 'position']]
    for col in df.columns:
    	df[col] = [0 if d<0 else d for d in df[col]]
    return df       

def cv_rookie_model(df):
	'''10 fold cross validation of Random Forest classifier examining AUC.'''
	X = df[ncaa_mean+['position']+draft_combine+basics].fillna(-1)
	X = preprocessing.scale(X) 
	y = df['kmeans_k2'].fillna(-1)
	cv = cross_validation.KFold(len(X), n_folds=10, indices=False)

	rf = RandomForestClassifier(n_estimators=100, criterion='entropy', max_features=30)
	results = []
	for traincv, testcv in cv:
	    rf.fit(X[traincv], y[traincv])
	    probas_ = rf.predict_proba(X[testcv])
	    fpr, tpr, thresholds = roc_curve(y[testcv], probas_[:, 1])
	    results.append(auc(fpr, tpr))
	print 'RF Average AUC with k=10 cross validation: '+str(np.array(results).mean())

def predict_rookies(train, test):
	'''Given a training and test data set, fits training set to model and predicts on test set.
	Returns test predictions.'''
	X_train = train[ncaa_mean+['position']+draft_combine+basics].fillna(-1)
	X_train = preprocessing.scale(X) 
	y_train = train['kmeans_k2'].fillna(-1)

	X_test = test[ncaa_mean+['position']+draft_combine+basics].fillna(-1)
	X_test = preprocessing.scale(X_test) 

	rf = RandomForestClassifier(n_estimators=100, criterion='entropy', max_features=30)
	rf.fit(X_train, y_train)
	y_pred = rf.predict(X_test)
	return y_pred
