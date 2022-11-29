##############################################################################
# Import necessary modules
# #############################################################################

import mlflow
import mlflow.sklearn
from airflow.utils.dates import days_ago
import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from datetime import datetime, timedelta
import importlib.util
import warnings
warnings.filterwarnings("ignore")


def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

utils = module_from_file("utils", "/home/airflow/dags/Lead_scoring_inference_pipeline/utils.py")
constants = module_from_file("constants", "/home/airflow/dags/Lead_scoring_inference_pipeline/constants.py")

DB_PATH = constants.DB_PATH
DB_FILE_NAME = constants.DB_FILE_NAME
FEATURES_TO_ENCODE = constants.FEATURES_TO_ENCODE
MODEL_NAME = constants.MODEL_NAME
STAGE = constants.STAGE
TRACKING_URI = constants.TRACKING_URI
ONE_HOT_ENCODED_FEATURES = constants.ONE_HOT_ENCODED_FEATURES


###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'upgrad_demo',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'provide_context': True
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@monthly',#changed from hourly to monthly as do not want this to run every hour
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################
encoding_categorical_variables = PythonOperator(task_id='encoding_categorical_variables', 
                            python_callable=utils.encode_features,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'features_to_encode': FEATURES_TO_ENCODE},
                            dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
generating_models_prediction = PythonOperator(task_id='generating_models_prediction', 
                            python_callable=utils.get_models_prediction,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'model_name': MODEL_NAME, 'model_stage': STAGE,'tracking_uri': TRACKING_URI},
                            dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
checking_model_prediction_ratio = PythonOperator(task_id='checking_model_prediction_ratio', 
                            python_callable=utils.prediction_ratio_check,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME},
                            dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
checking_input_features = PythonOperator(task_id='checking_input_features', 
                            python_callable=utils.input_features_check,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'ONE_HOT_ENCODED_FEATURES': ONE_HOT_ENCODED_FEATURES},
                            dag=Lead_scoring_inference_dag)


###############################################################################
# Define relation between tasks
# ##############################################################################

encoding_categorical_variables.set_downstream(checking_input_features)
checking_input_features.set_downstream(generating_models_prediction)
generating_models_prediction.set_downstream(checking_model_prediction_ratio)
