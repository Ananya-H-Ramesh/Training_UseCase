import logging
from .connectivity import *


class Validation:
    '''This Class is Created to Validate the Standardizations'''
  
    def __init__(self):
        self.connector = Connectivity.snowflake_connection()
        self.cursor = self.connector.cursor()

    def validate_zipcode(self):
        """
        Validate the zipcodes in the 'callcap' table using a SQL query.

        Executes a SQL query to check if all the zipcodes in the 'callcap' table are standardized.
        The validation result is logged using 'logging.info'.

        Returns:
            None
        """

        self.cursor.execute(
         ''' SELECT CASE
             WHEN count(*)=0 THEN 'All Zipcodes are Standardized'
             ELSE 'Not all zipcodes are Standardized'
             END AS validation_result
             FROM callcap
             WHERE REGEXP_LIKE(payload:caller_zipcode, '^\\\\d{5}$') = 0;
        ''')
        result = self.cursor.fetchone()
        validation_result = result[0]
        logging.info(validation_result)

    def validate_null(self):
        """
            Validate the presence of null values in the 'callcap' table using a SQL query.

            The validation result is logged using the 'logging.info' function.

            Returns:
                None
        """
        
        self.cursor.execute(
            '''
            SELECT CASE 
                WHEN COUNT(*)=0 THEN 'All Null Valued are Handled'
                ELSE 'There are Null values present'
                END AS validation_result
            FROM callcap, 
            lateral flatten(input => payload) b
            where b.value::string   = 'NULL' or b.value::string   = 'NA' or b.value::string   = 'N/A' or b.value::string ='NaN' or b.value::string = 'nan' or b.value::string = '';
            '''
        )
        result = self.cursor.fetchone()
        validation_result = result[0]
        logging.info(validation_result)

    def validate_column_names(self):
        """
        Validate the column names in the 'callcap' table using a SQL query.
        Returns:
            None
        """
        
        self.cursor.execute(
            '''
            SELECT CASE
            WHEN COUNT(*) = 0 THEN 'All keys in payloads are in snake case'
            ELSE 'All keys in payloads are not in snake case'
            END AS validation_result
            FROM callcap,
            LATERAL FLATTEN(input => OBJECT_KEYS(PARSE_JSON(payload))) f
            WHERE NOT REGEXP_LIKE(f.value, '^[a-z]+(_[a-z]+)*$');
            '''
        )
        result = self.cursor.fetchone()
        validation_result = result[0]
        logging.info(validation_result)

    def validate_date_id(self):
        """
            Validate the 'date_id' field in the 'callcap' table using a SQL query.

            Returns:
                None
        """
        
        self.cursor.execute(
            '''
                select REGEXP_REPLACE(CAST(payload:start_datetime as date),'-','')as date_callcap,date_id, 
                date_callcap=date_id as date_id_validation from callcap
                where date_id_validation = False;
            '''
        )
        rows_count = self.cursor.rowcount
        if rows_count == 0:
                logging.info("Date Id Validation Successful")
                
        else:
                logging.info("Date id did not pass validation")

    def validate_hour_id(self):
        """
            Validate the 'hour_id' field in the 'callcap' table using a SQL query.

            Returns:
                None
        """
        
        self.cursor.execute(
            '''
            SELECT RPAD(TO_CHAR(payload:start_datetime::TIMESTAMP, 'HH24'), 6, '0') AS hour_id_callcap,
            hour_id_callcap = hour_id AS validation_result
            FROM callcap
            WHERE validation_result = FALSE;
            '''
        )
        rows_count = self.cursor.rowcount
        if rows_count == 0:
                logging.info("Hour Id Validation Successful")
                
        else:
                logging.info("Hour id did not pass validation")

    def execute_validation(self):
        '''
         Execute the data Validation process 
        '''

        self.validate_zipcode()
        self.validate_null()
        self.validate_column_names()
        self.validate_date_id()
        self.validate_hour_id()

        