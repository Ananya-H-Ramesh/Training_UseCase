from . import params as config
import snowflake.connector
import logging
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
class Connectivity:
    '''Connectivity Class is Used to establish Connection with Snowflake'''

    def snowflake_connection():
        '''Connecting to Snowflake'''
       
        connector = snowflake.connector.connect(
            user = config.user,
            password = config.password,
            account = config.account,
            database = config.database,
            schema = config.schema
        )
        return connector

        
        
        
            
