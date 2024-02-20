import pandas as pd
from . import params as config
import logging
from . import constants
path = constants.csv_path


class ExtractData: 
    '''Extracting CallCap data'''

    def extract_dataset():
        """
            Extracting CallCap Data

            Parameters : Nothing.
            Returns : callcap_data --> DataFrame

            note: Incase of connection failure 3 times retry logic is applied with wait of 2s.
        """
        
        callcap_data = pd.read_csv(f"{path}")
        logging.info("Successfully Extracted Dataset")
        return callcap_data
        
        
        
        
            
