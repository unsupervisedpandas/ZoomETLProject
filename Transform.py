import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import pandas as pd

from Log import Log


class Transform:
    """
    A class to transform Zoom data before loading into Snowflake.

    Methods
    _______
    transform_data(df):
        Transforms the data in the dataframe parameter.
    transform_call_log(df):
        Transforms Zoom call log data.
    get_max_snowflake_time():
        Gets the most recent call time currently in Snowflake.
    """

    def __init__(self, data_config):
        """
        Constructs all the necessary attributes for the transform object.

        Parameters
        __________
            data_config : dict
                dictionary containing ETL specifications
        """

        # get Snowflake parameters from data_config
        self.USER = data_config['load']['user']
        self.PASSWORD = data_config['load']['password']
        self.ACCOUNT = data_config['load']['account']
        self.WAREHOUSE = data_config['load']['warehouse']
        self.DATABASE = data_config['load']['database']
        self.SCHEMA = data_config['load']['schema']

        # initialize logger
        self.log = Log('zoom_log', data_config)
        self.logger = self.log.get_logger()

    def transform_data(self, df):
        """
        Transforms the data in the dataframe parameter.

        Parameters
        __________
        df : Pandas DataFrame
            dataframe containing Zoom call data

        Returns
        _______
        df : Pandas DataFrame
            dataframe containing transformed Zoom call data
        """

        # transform data
        df = self.transform_call_log(df)

        return df

    def transform_call_log(self, df):
        """
        Transforms Zoom call log data.
        Parameters
        __________
        df : Pandas DataFrame
            dataframe containing Zoom call data

        Returns
        _______
        df : Pandas DataFrame
            dataframe containing transformed Zoom call data
        """

        # rename id to record_id, since id is a keyword in Snowflake
        df = df.rename(columns={'id': 'record_id'})

        # make columns upper case
        df.columns = map(lambda x: str(x).upper(), df.columns)

        # get subset of data that is not in Snowflake

        # get max datetime in Snowflake
        max_datetime = self.get_max_snowflake_time()

        # log max_datetime
        self.logger.info(f'The most recent datetime in Snowflake prior to load was: {max_datetime}')

        # select only Zoom data that is after the max time in Snowflake
        df = df[pd.to_datetime(df['DATE_TIME']) > str(max_datetime)]

        # log some summary stats about the transformed dataframe
        self.logger.info(
            f"The transformed dataframe has max datetime of {df['DATE_TIME'].max()},"
            f"min datetime of {df['DATE_TIME'].min()},"
            f"{df.shape[0]} rows,"
            f"and {df.shape[1]} columns.")

        return df

    def get_max_snowflake_time(self):
        """
        Gets the most recent call time currently in Snowflake.

        Returns
        _______
        max_datetime : datetime
            the most recent datetime in the date_time column of Zoom_Call_Logs in Snowflake
        """

        # connect to Snowflake
        conn = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT
        )

        # get cursor
        cur = conn.cursor()

        # specify the warehouse, database, and schema to use when creating the table
        try:
            cur.execute(f'USE WAREHOUSE {self.WAREHOUSE}')
            cur.execute(f'USE DATABASE {self.DATABASE}')
            cur.execute(f'USE SCHEMA {self.SCHEMA}')
        except ProgrammingError as er:
            print(f'The Warehouse, Database, or Schema does not exist\n {er}')
            raise

        # query Snowflake to get most recent date in the date_time column
        query = cur.execute('SELECT MAX(DATE_TIME)'
                            'FROM ZOOM_CALL_LOGS')

        # get the single result from the previous query
        max_datetime = query.fetchone()[0]

        return max_datetime
