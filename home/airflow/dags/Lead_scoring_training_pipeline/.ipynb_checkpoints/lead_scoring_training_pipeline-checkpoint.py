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


mlflow.set_tracking_uri("http://0.0.0.0:6006")


def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

utils = module_from_file("utils", "/home/airflow/dags/Lead_scoring_training_pipeline/utils.py")
constants = module_from_file("constants", "/home/airflow/dags/Lead_scoring_training_pipeline/constants.py")


MLFLOW_DB_PATH = constants.MLFLOW_DB_PATH
DB_FILE_MLFLOW = constants.DB_FILE_MLFLOW
DATA_PIPELINE_DB_PATH = constants.DATA_PIPELINE_DB_PATH
DB_FILE_NAME = constants.DB_FILE_NAME
FEATURES_TO_ENCODE = constants.FEATURES_TO_ENCODE


# Creating an experiment
try:
    logging.info("Creating mlflow experiment")
    mlflow.create_experiment(constants.EXPERIMENT)
except:
    pass


# Setting the environment with the created experiment
mlflow.set_experiment(constants.EXPERIMENT)

###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'upgrad_demo',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'provide_context': True
}

ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for create_MLlow_DB_sqlit_connection() function with task_id 'create_mlflow_db_sqlit_connection'
# ##############################################################################
create_mlflow_db_sqlit_connection = PythonOperator(task_id='create_mlflow_db_sqlit_connection', 
                            python_callable=utils.create_MLflow_DB_sqlit_connection,
                            op_kwargs={'db_path': MLFLOW_DB_PATH, 'db_file': DB_FILE_MLFLOW},
                            dag=ML_training_dag)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################
encoding_categorical_variables = PythonOperator(task_id='encoding_categorical_variables', 
                            python_callable=utils.encode_features,
                            op_kwargs={'db_path': DATA_PIPELINE_DB_PATH, 'db_file_name': DB_FILE_NAME, 'features_to_encode': FEATURES_TO_ENCODE},
                            dag=ML_training_dag)


###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################

training_model = PythonOperator(task_id='training_model', 
                            python_callable=utils.get_trained_model,
                            op_kwargs={'db_path': DATA_PIPELINE_DB_PATH, 'db_file_name': DB_FILE_NAME},
                            dag=ML_training_dag)

###############################################################################
# Define relations between tasks
# ##############################################################################

create_mlflow_db_sqlit_connection.set_downstream(encoding_categorical_variables)
encoding_categorical_variables.set_downstream(training_model)
