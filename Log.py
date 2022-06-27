import logging
import traceback
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


class Log:
    """
    A class to log ETL tasks

    ...

    Methods
    _______

    """

    def __init__(self, logger_name, data_config, stream=False):
        """
        Constructs all the necessary attributes for the Load object.

        Parameters
        __________
        logger_name : str
            the name of the logger object
        data_config : dict
            dictionary containing ETL specifications
        stream : bool
            if true, the logger will log to the console, else logs to a file
        """

        # file path where log will be located
        self.file_path = json.load(open('data_config.json'))['log']['file_path']

        # create and configure logger object
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d | %(threadName)s | %(filename)s | %(lineno)d | %(funcName)s() | %(levelname)s | '
            '%(message)s', '%Y-%m-%d %H:%M:%S')

        if stream:
            handler = logging.StreamHandler()
        else:
            handler = logging.FileHandler(self.file_path)

        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)

        # if the logger does not already have a handler, add the one we created
        if not self.logger.handlers:
            self.logger.addHandler(handler)

        # names of columns for log
        self.col_names = data_config['log']['col_names']

        # get Snowflake parameters from data_config
        self.USER = data_config['load']['user']
        self.PASSWORD = data_config['load']['password']
        self.ACCOUNT = data_config['load']['account']
        self.WAREHOUSE = data_config['load']['warehouse']
        self.DATABASE = data_config['log']['database']
        self.SCHEMA = data_config['log']['schema']
        self.TABLE_NAME = data_config['log']['table_name']

        # datetime columns in log file
        self.datetime_columns = data_config['log']['datetime_columns']

    def exception_logging(self, exctype, value, tb):
        """
        Method of handling exceptions by logging them, rather than outputing them to the console.
        Pameters
        ________
        exctype :
        value :
        tb :

        Returns
        _______
        None
        """

        # log exception
        exception_string = f'{{exception_type: {exctype}, trcbk: {traceback.format_tb(tb)}, value: {value}}}'
        self.logger.error(exception_string)

        # load log into Snowflake
        # self.load_log()

        # close log
        self.close_log()

    def close_log(self):
        """
        Closes the current logger.

        Returns
        _______
        None
        """
        # remove and close handlers
        handlers = self.logger.handlers[:]
        for handler in handlers:
            self.logger.removeHandler(handler)
            handler.close()

    def load_log(self):
        """
        Loads the log file into Snowflake.
        Returns
        _______
        None
        """

        # connect to Snowflake
        conn = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT
        )

        # get Snowflake cursor
        cur = conn.cursor()

        # create warehouse, db, and schema if not exists
        cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {self.WAREHOUSE}')
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {self.DATABASE}')
        cur.execute(f'USE DATABASE {self.DATABASE}')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {self.SCHEMA}')

        # specify the warehouse, database, and schema to use when creating the table
        cur.execute(f'USE WAREHOUSE {self.WAREHOUSE}')
        cur.execute(f'USE DATABASE {self.DATABASE}')
        cur.execute(f'USE SCHEMA {self.SCHEMA}')

        # read the log file into a pandas dataframe
        file_path = self.file_path
        col_names = self.col_names
        log_df = pd.read_table(file_path, sep='|', header=None, names=col_names)

        # append log to the end of the log table
        success, num_chunks, num_rows, _ = write_pandas(conn, log_df, self.TABLE_NAME, self.DATABASE, self.SCHEMA,
                                                        quote_identifiers=False)

        # close connector
        conn.close()

    def create_log_table(self, load):
        """
        Creates a Snowflake table to store log data.

        Parameters
        __________
        log : Log object
             Log object from the Log class
        load : Load object
            object for loading data into Snowflake

        Returns
        _______
        None
        """

        # connect to Snowflake
        conn = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT
        )

        # get cursor
        cur = conn.cursor()

        # create warehouse, db, and schema if not exists
        cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {self.WAREHOUSE}')
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {self.DATABASE}')
        cur.execute(f'USE DATABASE {self.DATABASE}')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {self.SCHEMA}')

        # specify the warehouse, database, and schema to use when creating the table
        cur.execute(f'USE WAREHOUSE {self.WAREHOUSE}')
        cur.execute(f'USE DATABASE {self.DATABASE}')
        cur.execute(f'USE SCHEMA {self.SCHEMA}')

        # read log file into pandas dataframe
        file_path = self.file_path
        col_names = self.col_names
        log_df = pd.read_table(file_path, sep='|', header=None, names=col_names)

        # make create string using the load function, and create table
        create = load.get_create_string(self.TABLE_NAME, log_df, self.datetime_columns)
        cur.execute(create)

        # close connector
        conn.close()

    def get_logger(self):
        """returns logger class variable"""
        return self.logger


