"""
Import necessary modules
############################################################################## 
"""


import os 
import sqlite3
from sqlite3 import Error

import importlib.util

import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

'''
def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

constants = module_from_file("utils", "/home/airflow/dags/Lead_scoring_data_pipeline/constants.py")
schema = module_from_file("schema", "/home/airflow/dags/Lead_scoring_data_pipeline/schema.py")

db_path = constants.DB_PATH
db_file_name = constants.DB_FILE_NAME
raw_data_schema = schema.raw_data_schema 
model_input_schema = schema.model_input_schema
'''

def read_data_from_database(db_path, db_file_name, table_name):
    cnx = sqlite3.connect(db_path+db_file_name)
    df = pd.read_sql('select * from '+table_name, cnx)
    cnx.close()
    return df


###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check(db_path, db_file_name, raw_data_schema, data_directory):
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be   
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
   
    df = pd.read_csv(data_directory)
    if len(raw_data_schema) == df.shape[1]:
        print("Raw datas schema is in line with the schema present in schema.py")
    else:
        print("Raw datas schema is NOT in line with the schema present in schema.py")
    
    

###############################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check(db_path, db_file_name, model_input_schema):
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be   
        raw_data_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    
    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from model_input', cnx)
    
    df = read_data_from_database(db_path, db_file_name, "model_input")
    
    if len(model_input_schema) == df.shape[1]:
        print("Models input schema is in line with the schema present in schema.py")
    else:
        print("Models input schema is NOT in line with the schema present in schema.py")
    
