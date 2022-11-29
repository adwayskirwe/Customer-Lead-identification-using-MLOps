'''
filename: utils.py
functions: encode_features, load_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3

import os
import logging
import collections

from datetime import datetime



def read_data_from_database(db_path, db_file_name, table_name):
    cnx = sqlite3.connect(db_path+db_file_name)
    df = pd.read_sql('select * from '+table_name, cnx)
    cnx.close()
    return df


def save_data_to_database(db_path, db_file_name, table_name, df):
    cnx = sqlite3.connect(db_path+db_file_name)
    df.to_sql(name=table_name, con=cnx, if_exists='replace', index=False)
    cnx.close()

###############################################################################
# Define the function to train the model
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
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
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
    
    #target = df['app_complete_flag']
    features = df.drop(['first_platform_c','first_utm_medium_c','first_utm_source_c'], axis = 1)
    features["first_utm_medium_c_Level10"] = 0
    
    #features.to_sql(name='features', con=cnx, if_exists='replace', index=False)
    save_data_to_database(db_path, db_file_name, "features", features)
    #target.to_sql(name='target', con=cnx, if_exists='replace', index=False)

###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def get_models_prediction(db_path, db_file_name, model_name, model_stage, tracking_uri):
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    mlflow.set_tracking_uri(tracking_uri)
    
    #cnx = sqlite3.connect(db_path+db_file_name)
    #X = pd.read_sql('select * from features', cnx)
    X = read_data_from_database(db_path, db_file_name, "features")
    
    model_uri = f"models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    
    predictions = loaded_model.predict(X)
    predicted_output = pd.DataFrame(predictions, columns=['predicted_output']) 
    #predicted_output.to_sql(name='predicted_output', con=cnx, if_exists='replace', index=False)
    save_data_to_database(db_path, db_file_name, "predicted_output", predicted_output)

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check(db_path,db_file_name):
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''

    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from predicted_output', cnx)
    df = read_data_from_database(db_path, db_file_name, "predicted_output")
    zeros=round(df["predicted_output"].value_counts()[0]/(df["predicted_output"].value_counts()[0]+df["predicted_output"].value_counts()[1]),2)
    ones=round(df["predicted_output"].value_counts()[1]/(df["predicted_output"].value_counts()[0]+df["predicted_output"].value_counts()[1]),2)
    prediction_distribution_df = pd.DataFrame({'0':[zeros],'1':[ones]})
    
    filename = 'prediction_distribution'+ datetime.now().strftime("%Y%m%d%H%M%S")+'.csv'
    prediction_distribution_df.to_csv("/home/airflow/dags/Lead_scoring_inference_pipeline/"+filename)
###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_features_check(db_path, db_file_name, ONE_HOT_ENCODED_FEATURES):
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    
    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from features', cnx)
    df = read_data_from_database(db_path, db_file_name, "features")
    if df.shape[1] == len(ONE_HOT_ENCODED_FEATURES):
        if collections.Counter(df.columns) == collections.Counter(ONE_HOT_ENCODED_FEATURES):
            return 'All the models input are present' 
        else: 
            return 'Some of the models inputs are missing'
    else:
        return 'Some of the models inputs are missing'
