import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_fixed


class StandardizeData: 
    '''This Class is Used to Standardize Data'''
    
    def null_standardization(data):
        """
        Args:
            data (pandas.DataFrame): The DataFrame in which NaN values should be replaced.

        Returns:
            None
        """
        
        data.replace(float('nan'), None, inplace=True)

    def snake_case(data):
        """
        Converts column names to snake_case in a pandas DataFrame.
        Args:
            data (pandas.DataFrame): The DataFrame in which column names should be converted to snake_case.

        Returns:
            None
        """
        
        data.rename(columns=str.lower, inplace=True)

    def zipcode_transformation(data):
        """
        Performs a transformation on the caller_zipcode column in a pandas DataFrame.
        Args:
            data (pandas.DataFrame): The DataFrame in which the caller_zipcode column needs to be transformed.

        Returns:
            None
        """
        
        def fix_zip(zip_code):
            return zip_code.astype(str).str.extract('(\d+)', expand=False).str.zfill(5)

        data['caller_zipcode'] = fix_zip(data['caller_zipcode'])

    def dateid_and_hourid_generation(data):
        """
        Generate date_id and hour_id columns from the datetime column in a pandas DataFrame.
        Args:
        data (pandas.DataFrame): The DataFrame in which the date_id and hour_id columns need to be generated.

        Returns:
             None
        """
        
        data['date_id'] = pd.to_datetime(data['start_datetime']).dt.strftime('%Y%m%d')
        data['hour_id'] = pd.to_datetime(data['start_datetime']).dt.strftime('%H').apply(lambda x: x.lstrip('0') or '0') + '0000'

    def execute_standardizations(data):
        """
        Executes the data Standardizations process 
        
        """
        
        StandardizeData.null_standardization(data)
        StandardizeData.snake_case(data)
        StandardizeData.zipcode_transformation(data)
        StandardizeData.dateid_and_hourid_generation(data)
        logging.info("All Standardizations are applied on Callcap data")

        
        
        
            
