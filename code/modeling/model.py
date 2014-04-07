import pandas as pd
import seaborn as sns

from sklearn.cluster import KMeans
from sklearn import preprocessing
from sklearn import cross_validation
from sklearn.metrics import confusion_matrix, roc_curve, auc
from sklearn.ensemble import RandomForestClassifier

def cluster_players(df):
    '''Performs k-means w/ k=2 clustering of fantasy players on pts and pts/game
    Returns updated dataframe with cluster IDs'''
    
    kmeans = KMeans(n_clusters=2).fit(df[['ffpts', 'ffpts_gp']])
    labels_2 = kmeans.labels_
    for lab in unique(labels_2):
        print lab, len(labels_2[labels_2==lab])*1.0/len(labels_2)
    df['kmeans_k2'] = labels_2
    
    #   sns.boxplot(df['ffpts'], groupby=labels_2)
    #   plt.savefig('charts/cluster_results.png')
    return df


def map_features(df):
    '''Maps out categoricals to numerics for use in modeling
    Returns updated dataframe'''
    
    # replace 0s which should be nan with nans
    df = df.replace(0, nan)
    features = pd.DataFrame()
    
    # map features
    for col in df.columns:
        if df[col].dtype == np.dtype('object'):
            s = np.unique(df[col].fillna(-1).values)
            mapping = pd.Series([x[0] for x in enumerate(s)], index = s)
            features = pd.concat((features, pd.DataFrame(df[col].map(mapping).fillna(-1))), axis=1)
        else:
            features = pd.concat((features, pd.DataFrame(df[col].fillna(-1))), axis=1)
    
    # replace mapped out categoricals in df
    df[['class', 'college', 'conference', 'subn', 'Home Country', 'Home State', 'Home Town', 
        'Last School', 'position']] = \
    features[['class', 'college', 'conference', 'subn', 'Home Country', 'Home State', 'Home Town', 
        'Last School', 'position']]
    return df  


# globals
draft_combine = ['draft_pick', 'height_combine', 'weight_combine', 'wonderlic', 'forty_yard', 
    'bench_press', 'vertical_leap', 'broad_jump', 'shuttle', 'three_cone']

ncaa_mean = ['Pass Att_mean', 'Pass Comp_mean', 'Pass Conv_mean', 'Pass Int_mean', 
    'Pass TD_mean', 'Pass Yard_mean', 
    'QB Hurry_mean', 'Rec_mean', 'Rec TD_mean', 'Rec Yards_mean', 
    'Rush Att_mean', 'Rush TD_mean', 'Rush Yard_mean', 'Fumble_mean', 'Fumble Lost_mean', 
    'Kickoff Ret_mean', 'Kickoff Ret TD_mean', 'Kickoff Ret Yard_mean', 'Kickoff Touchback_mean', 
    'Punt Ret_mean', 'Punt Ret TD_mean', 'Punt Ret Yard_mean', 'Off 2XP Att_mean', 'Off 2XP Made_mean', 
    'Points_mean', 'Tackle Assist_mean', 'Tackle For Loss_mean', 'Tackle For Loss Yard_mean', 
    'Tackle Solo_mean', 'Pass Broken Up_mean', 'Sack_mean', 'Sack Yard_mean', 
    'Int Ret_mean', 'Int Ret TD_mean', 'Int Ret Yard_mean', 
    'Misc Ret_mean', 'Misc Ret TD_mean', 'Misc Ret Yard_mean', 'Kick/Punt Blocked_mean', 
    'Fumble Forced_mean', 'Fum Ret_mean', 'Fum Ret TD_mean', 'Fum Ret Yard_mean', 
    'Def 2XP Att_mean', 'Def 2XP Made_mean', 'Safety_mean', 
    'Field Goal Att_mean', 'Field Goal Made_mean', 'Off XP Kick Att_mean', 'Off XP Kick Made_mean', 
    'Punt_mean', 'Punt Yard_mean', 'Kickoff_mean', 'Kickoff Yard_mean', 
    'Kickoff Onside_mean', 'Kickoff Out-Of-Bounds_mean']
    
basics = ['class', 'college', 'conference', 'subn', 
    'height_ncaa', 'weight_ncaa', 'Home Country', 'Home State', 'Home Town', 'Last School']


def cross_validate_model(df):
    '''10 fold cross validation of tuned random forest classifier examining AUC'''
    
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
    '''Given a training and test data set, fits training set to model and predicts on test set
    Returns test predictions'''
    
    X_train = train[ncaa_mean+['position']+draft_combine+basics].fillna(-1)
    X_train = preprocessing.scale(X) 
    y_train = train['kmeans_k2'].fillna(-1)

    X_test = test[ncaa_mean+['position']+draft_combine+basics].fillna(-1)
    X_test = preprocessing.scale(X_test) 

    rf = RandomForestClassifier(n_estimators=100, criterion='entropy', max_features=30)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    return y_pred
