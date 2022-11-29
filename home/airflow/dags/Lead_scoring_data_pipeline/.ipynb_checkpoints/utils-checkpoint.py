##############################################################################
# Import necessary modules and files
# #############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error


def load_data(file_path_list):
    data = []
    file_name = os.path.basename(file_path_list[0])
    for eachfile in file_path_list:
        if file_name == "leadscoring_test.csv":
            data.append(pd.read_csv(eachfile))
        else:
            data.append(pd.read_csv(eachfile, index_col=[0]))
    return data


def check_if_table_has_value(cnx,table_name):
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", cnx).shape[0]
    if check_table == 1:
        return True
    else:
        return False

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
# Define the function to build database
# ##############################################################################

def build_dbs(db_path, db_file_name):
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '   


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    

    if os.path.isfile(db_path+db_file_name):
        print( "DB already exists")
        print(os.getcwd())
        return "DB Exists"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            
            conn = sqlite3.connect(db_path+db_file_name)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"
            

###############################################################################
# Define function to load the csv file to the database
# ##############################################################################

def load_data_into_db(db_path, db_file_name, data_directory):
    '''
    Thie function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''

    cnx = sqlite3.connect(db_path+db_file_name)
    
    if not check_if_table_has_value(cnx,'train'):
        print("Table Doesn't Exsist - train, Building")
        df = load_data( [f"{data_directory}",
                   ]
                 )[0]
        
        df['total_leads_droppped'] = df['total_leads_droppped'].fillna(0)
        df['referred_lead'] = df['referred_lead'].fillna(0)
        
        df.to_sql(name='loaded_data', con=cnx, if_exists='replace', index=False)
        
    
    cnx.close()
    return "Writing to DataBase Done or Data Already was in Table. Check Logs."

###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################

    
def map_city_tier(db_path, db_file_name, city_tier_mapping):
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in /mappings/city_tier_mapping.py file. If a
    particular city's tier isn't mapped in the city_tier_mapping.py then
    the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from loaded_data', cnx)
    df = read_data_from_database(db_path, db_file_name, "loaded_data")
    #print(f"Connected to = {db_file_name} in {db_path}")
    df["city_tier"] = df["city_mapped"].map(city_tier_mapping)
    df["city_tier"] = df["city_tier"].fillna(3.0)
    print("Mapping done")
    
    df = df.drop(['city_mapped'], axis = 1)
    print("Dropped column city_mapped successfully")
    #df.to_sql(name='city_tier_mapped', con=cnx, if_exists='replace', index=False)
    #cnx.close()
    save_data_to_database(db_path, db_file_name, "city_tier_mapped", df)

    
    
    
    
###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars(db_path, db_file_name, list_platform, list_medium, list_source):
    '''
    This function maps all the unsugnificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''

    #cnx = sqlite3.connect(db_path+db_file_name)
    #df_lead_scoring = pd.read_sql('select * from city_tier_mapped', cnx)
    df_lead_scoring = read_data_from_database(db_path, db_file_name, "city_tier_mapped")
    
    # first_platform_c *********** all the levels below 90 percentage are assgined to a single level called others
    new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in list_platform
    new_df['first_platform_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # first_utm_medium_c ************* all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
    new_df['first_utm_medium_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # first_utm_source_c ************* all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
    new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    
    #df.to_sql(name='categorical_variables_mapped', con=cnx, if_exists='replace', index=False)
    #cnx.close()
    save_data_to_database(db_path, db_file_name, "categorical_variables_mapped", df)
    
    
    

##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping(db_path, db_file_name, interaction_mapping_file, index_columns):
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        interaction_mapping_file : path to the csv file containing interaction's
                                   mappings
        index_columns : list of columns to be used as index while pivoting and
                        unpivoting
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our index_columns. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' in it as index_column else pass a list without 'app_complete_flag'
        in it.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
   
    #cnx = sqlite3.connect(db_path+db_file_name)
    #df = pd.read_sql('select * from categorical_variables_mapped', cnx)
    df = read_data_from_database(db_path, db_file_name, "categorical_variables_mapped")
    
    df = df.drop_duplicates()
    #df_event_mapping = pd.read_csv('Maps/interaction_mapping.csv', index_col=[0])
    #df_event_mapping = pd.read_csv(interaction_mapping_file, index_col=[0])
    df_event_mapping = interaction_mapping_file
    
    # unpivot the interaction columns and put the values in rows
    df_unpivot = pd.melt(df, id_vars=index_columns, var_name='interaction_type', value_name='interaction_value')
    
    # handle the nulls in the interaction value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)

    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')

    #dropping the interaction type column as it is not needed
    df = df.drop(['interaction_type'], axis=1)

    # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(
            values='interaction_value', index=index_columns, columns='interaction_mapping', aggfunc='sum')
    
    df_pivot = df_pivot.reset_index()
    
    #df_pivot.to_sql(name='interactions_mapped', con=cnx, if_exists='replace', index=False)
    save_data_to_database(db_path, db_file_name, "interactions_mapped", df)
    
    df = df_pivot.drop(['created_date','assistance_interaction','career_interaction','payment_interaction','social_interaction','syllabus_interaction'], axis = 1)
    
    
    #df.to_sql(name='model_input', con=cnx, if_exists='replace', index=False)
    #cnx.close()
    save_data_to_database(db_path, db_file_name, "model_input", df)
    
    
    
