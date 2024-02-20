import logging
from .main import ExecuteLoadCallcap
from . import constants
from .utils import *
from . import params

'''Entry Point to Pipeline'''
try:
    
    Snowflake_load = ExecuteLoadCallcap()
    Snowflake_load.execute()
    logging.info("Callcap Data Successfully loaded to Snowflake and Validated")

except Exception as err:

    #Sending Slack Notification on Failure
    failure_message = f"Pipeline Failed With Error {err}"
    send_slack_notification(params.slack_token, params.slack_channel_failure, failure_message)
    logging.error("Error: %s", err)
    raise Exception(err)
