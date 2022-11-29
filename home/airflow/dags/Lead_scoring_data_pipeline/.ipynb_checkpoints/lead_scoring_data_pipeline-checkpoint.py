##############################################################################
# Import necessary modules
# #############################################################################


from airflow.utils.dates import days_ago
import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from datetime import datetime, timedelta
import importlib.util


interaction_mapping = pd.read_csv("/home/airflow/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv")

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

utils = module_from_file("utils", "/home/airflow/dags/Lead_scoring_data_pipeline/utils.py")
data_validation_checks = module_from_file("data_validation_checks", "/home/airflow/dags/Lead_scoring_data_pipeline/data_validation_checks.py")
constants = module_from_file("constants", "/home/airflow/dags/Lead_scoring_data_pipeline/constants.py")
schema = module_from_file("schema", "/home/airflow/dags/Lead_scoring_data_pipeline/schema.py")

city_tier_mapping = module_from_file("city_tier_mapping", "/home/airflow/dags/Lead_scoring_data_pipeline/mapping/city_tier_mapping.py")
#interaction_mapping = module_from_file("interaction_mapping", "/home/airflow/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv")
significant_categorical_level = module_from_file("significant_categorical_level", "/home/airflow/dags/Lead_scoring_data_pipeline/mapping/significant_categorical_level.py")


DB_PATH = constants.DB_PATH
DB_FILE_NAME= constants.DB_FILE_NAME
INDEX_COLUMNS = constants.INDEX_COLUMNS
DATA_DIRECTORY = constants.DATA_DIRECTORY
list_platform = significant_categorical_level.list_platform
list_medium = significant_categorical_level.list_medium
list_source = significant_categorical_level.list_source
raw_data_schema = schema.raw_data_schema
model_input_schema = schema.model_input_schema
city_tier_mapping = city_tier_mapping.city_tier_mapping

###############################################################################
# Define default arguments and DAG
# ##############################################################################

default_args = {
    'owner': 'upgrad_demo',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'provide_context': True
}

ML_data_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False,
                tags=['data_pipeline']
)


###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
# ##############################################################################

building_db = PythonOperator(task_id='building_db', 
                            python_callable=utils.build_dbs,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME},
                            dag=ML_data_dag)

###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
# ##############################################################################

checking_raw_data_schema = PythonOperator(task_id='checking_raw_data_schema', 
                            python_callable=data_validation_checks.raw_data_schema_check,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'raw_data_schema': raw_data_schema, 'data_directory':DATA_DIRECTORY},
                            dag=ML_data_dag)

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
# #############################################################################

loading_data = PythonOperator(task_id='loading_data', 
                            python_callable=utils.load_data_into_db,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'data_directory': DATA_DIRECTORY},
                            dag=ML_data_dag)

###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
# ##############################################################################

mapping_city_tier = PythonOperator(task_id='mapping_city_tier', 
                            python_callable=utils.map_city_tier,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'city_tier_mapping': city_tier_mapping},
                            dag=ML_data_dag)

###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
# ##############################################################################

mapping_categorical_vars = PythonOperator(task_id='mapping_categorical_vars', 
                            python_callable=utils.map_categorical_vars,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'list_platform':list_platform, 'list_medium':list_medium, 'list_source':list_source},
                            dag=ML_data_dag)

###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
# ##############################################################################

mapping_interactions = PythonOperator(task_id='mapping_interactions', 
                            python_callable=utils.interactions_mapping,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'interaction_mapping_file':interaction_mapping, 'index_columns':INDEX_COLUMNS},
                            dag=ML_data_dag)

###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
# ##############################################################################

checking_model_inputs_schema = PythonOperator(task_id='checking_model_inputs_schema', 
                            python_callable=data_validation_checks.model_input_schema_check,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': DB_FILE_NAME, 'model_input_schema':model_input_schema},
                            dag=ML_data_dag)

###############################################################################
# Define the relation between the tasks
# ##############################################################################

building_db.set_downstream(checking_raw_data_schema)
checking_raw_data_schema.set_downstream(loading_data)
loading_data.set_downstream(mapping_city_tier)
mapping_city_tier.set_downstream(mapping_categorical_vars)
mapping_categorical_vars.set_downstream(mapping_interactions)
mapping_interactions.set_downstream(checking_model_inputs_schema)

