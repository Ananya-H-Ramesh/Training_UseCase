from .connectivity import *
import logging
from . import constants
import datetime
path = constants.json_path
staging_callcap = constants.staging_callcap
final_callcap = constants.final_callcap
new_file_name =  str(
            datetime.datetime.now().strftime(r"%Y%m%d_%H%M%S") + "_CallCap"
        )

class LoadSnowflake:

    def __init__(self):
        self.connector = Connectivity.snowflake_connection()
        self.cursor = self.connector.cursor()

    def df_to_json(self,df):
        '''
        Converts a DataFrame to JSON format and saves it as a file.

        Parameters:
            df (pandas.DataFrame): The DataFrame to be converted to JSON.
            
        Returns:
            None
        '''
        
        df.to_json(f"{path}/{new_file_name}.json",orient='records')
        

    def put_to_stage(self,path,file_name):
        """
        Put a file to the internal stage.

        Args:
            self: The instance of the class.
            path (str): The path to the file to be loaded to the stage.

        Returns:
            None
        """

        self.cursor.execute(f"""PUT file://{path}/{file_name}.json @json_stage;""")
        logging.info("Loaded json data to Stage")

    def load_staging_table(self,table_name,file_name):

        """
        Load data from the internal stage to the landing table.
        Args:
            self: The instance of the class.
            table_name (str): The name of the landing table.

        Returns:
            None
        """
        
        self.cursor.execute(f"""
        COPY INTO {table_name}
        FROM '@json_stage/{file_name}.json.gz'
        FILE_FORMAT = (
            FORMAT_NAME = jsonfile
        )
        """)
        logging.info("Loaded json data from stage to landing table")

    def load_final_table(self,table_name,final_table):
        """
        Create and load data from the landing table to the final Callcap table.
        Args:
            self: The instance of the class.
            table_name (str): The name of the landing table.

        Returns:
            None
        """
        
        self.cursor.execute(f"""
        INSERT INTO {final_table} (date_id, hour_id, payload)
        SELECT
            json_data:date_id::NUMBER AS date_id,
            json_data:hour_id::NUMBER AS hour_id,
            json_data AS payload
        FROM
            {table_name};
        """)
        logging.info("Loaded data from Landing table to Final callcap Table")

    def truncate_staging_table(self,table_name):
        self.cursor.execute(f"""
        truncate table {table_name}
        """)
    

    def execute_loading(self,df):
        '''
         Execute the data loading process 
        '''
        self.df_to_json(df)
        self.put_to_stage(path,new_file_name)
        self.load_staging_table(staging_callcap,new_file_name)
        self.load_final_table(staging_callcap,final_callcap)
        self.truncate_staging_table(staging_callcap)
       

        