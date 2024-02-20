import pandas as pd
from . import params as config
from .transform import StandardizeData
from .extract import ExtractData
from .json_to_snowflake import LoadSnowflake
from .validation import Validation
from . import constants
from . import params
from . utils import *
import logging
path = constants.json_path


class ExecuteLoadCallcap:
    '''This Class Executes the Complete Pipeline'''

    def execute(self):
        
       # Extracting CallCap Data
        load_data = ExtractData
        df = load_data.extract_dataset()
            
        if df.empty:
            logging.info("DataFrame is Empty , There is no Data to Load into Snowflake")

            #Sending Slack Notification
            success_message = "DataFrame is Empty , There is no Data to Load into Snowflake"
            send_slack_notification(params.slack_token, params.slack_channel_success, success_message)
        else:
                
            # Applying Standardization to DataFrame
            StandardizeData.execute_standardizations(df)
                
            #Loading data to Snowflake 
            load_to_snowflake = LoadSnowflake()
            load_to_snowflake.execute_loading(df)

            #Validating the Standardizations
            validate = Validation()
            validate.execute_validation()

            #Sending Slack Notification on Success
            success_message = "CallCap data has been Extracted, Standardized , Loaded into Snowflake and Validated"
            send_slack_notification(params.slack_token, params.slack_channel_success, success_message)


                

       
