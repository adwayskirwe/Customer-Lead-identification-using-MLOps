'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np
import pickle
import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

import sklearn
from sklearn.model_selection import train_test_split

import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report,roc_auc_score,f1_score,confusion_matrix,accuracy_score,precision_score,recall_score
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from skopt import BayesSearchCV # run pip install scikit-optimize

import importlib.util
import warnings
warnings.filterwarnings("ignore")


def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

constants = module_from_file("constants", "/home/Assignment/02_training_pipeline/scripts/constants.py")


def read_data_from_database(db_path, db_file_name, table_name):
    cnx = sqlite3.connect(db_path+db_file_name)
    df = pd.read_sql('select * from '+table_name, cnx)
    cnx.close()
    return df


def save_data_to_database(db_path, db_file_name, table_name, df):
    cnx = sqlite3.connect(db_path+db_file_name)
    df.to_sql(name=table_name, con=cnx, if_exists='replace', index=False)
    cnx.close()

    
def create_MLflow_DB_sqlit_connection(db_path,db_file):
    """ create a database connection to a SQLite database """
    conn = None
    # opening the conncetion for creating the sqlite db
    try:
        conn = sqlite3.connect(db_path+db_file)
        print(sqlite3.version)
    # return an error if connection not established
    except Error as e:
        print(e)
    # closing the connection once the database is created
    finally:
        if conn:
            conn.close()
  
            
def get_validation_unseen_set(dataframe, validation_frac=0.05, sample=False, sample_frac=0.1):
    if not sample:
        dataset = dataframe.copy()
    else:
        dataset = dataframe.sample(frac=sample_frac)
    data = dataset.sample(frac=(1-validation_frac), random_state=786)
    data_unseen = dataset.drop(data.index)
    data.reset_index(inplace=True, drop=True)
    data_unseen.reset_index(inplace=True, drop=True)
    return data, data_unseen


###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features(db_path, db_file_name, features_to_encode):
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
    
    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from model_input', cnx)
    df = read_data_from_database(db_path, db_file_name, "model_input")
    first_platform_c=features_to_encode[0]
    df_dummy = pd.get_dummies(df[first_platform_c], prefix = 'first_platform_c', prefix_sep='_')
    df = pd.concat([df, df_dummy], axis=1)

    first_utm_medium_c=features_to_encode[1]
    df_dummy = pd.get_dummies(df[first_utm_medium_c], prefix = 'first_utm_medium_c', prefix_sep='_')
    df = pd.concat([df, df_dummy], axis=1)

    first_utm_source_c=features_to_encode[2]
    df_dummy = pd.get_dummies(df[first_utm_source_c], prefix = 'first_utm_source_c', prefix_sep='_')
    df = pd.concat([df, df_dummy], axis=1)
    
    target = df['app_complete_flag']
    features = df.drop(['app_complete_flag','first_platform_c','first_utm_medium_c','first_utm_source_c'], axis = 1)
    
    #features.to_sql(name='features', con=cnx, if_exists='replace', index=False)
    save_data_to_database(db_path, db_file_name, "features", features)
    #target.to_sql(name='target', con=cnx, if_exists='replace', index=False)
    save_data_to_database(db_path, db_file_name, "target", target)

###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model(db_path, db_file_name):
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''

    #data_pipeline_connection = sqlite3.connect(db_path+db_file_name)
    #X = pd.read_sql('select * from features', data_pipeline_connection)
    X = read_data_from_database(db_path, db_file_name, "features")
    #y = pd.read_sql('select * from target', data_pipeline_connection)
    y = read_data_from_database(db_path, db_file_name, "target")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.3, random_state = 0)
    
    model_config = constants.model_config
    #print(model_config)
    
    
    #mlflow.set_experiment(constants.EXPERIMENT)
    #experiment = mlflow.get_experiment_by_name(constants.EXPERIMENT)

    
    #with mlflow.start_run(experiment_id=experiment.experiment_id, run_name='run_LightGB') as mlrun:
    with mlflow.start_run(run_name='run_LightGB') as mlrun:
        #Model Training
        classifier = lgb.LGBMClassifier()
        classifier.set_params(**model_config) 
        classifier.fit(X_train, y_train)

        mlflow.sklearn.log_model(sk_model=classifier ,artifact_path="models", registered_model_name='LightGBM')
        mlflow.log_params(model_config)    

        # predict the results on training dataset
        y_pred=classifier.predict(X_test)

        # view accuracy
        acc=accuracy_score(y_test, y_pred)
        mlflow.log_metric('test_accuracy', acc)
        
        conf_mat = confusion_matrix(y_test, y_pred)
        tn = conf_mat[0][0]
        fn = conf_mat[1][0]
        tp = conf_mat[1][1]
        fp = conf_mat[0][1]
        
        mlflow.log_metric("True Negative", tn)
        mlflow.log_metric("False Negative", fn)
        mlflow.log_metric("True Positive", tp)
        mlflow.log_metric("False Positive", fp)
        
        auc_score = roc_auc_score(y_test, y_pred)
        mlflow.log_metric("AUC score", auc_score)
        
        precision = precision_score(y_pred, y_test,average= 'macro')
        mlflow.log_metric("Precision score", precision)
        
        recall = recall_score(y_pred, y_test, average= 'macro')
        mlflow.log_metric("Recall score", recall)
        
        f1 = f1_score(y_test, y_pred)
        mlflow.log_metric("f1", f1)


        
        
    
