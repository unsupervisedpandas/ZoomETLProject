import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta

from Log import Log


class Load:
    """
    A class to load data from pandas dataframes into Snowflake.

    ...

    Methods
    _______
    load_table(df, table_name):
        Loads Zoom data into a Snowflake table.
    get_create_string(table_name, df):
        Produces a SQL CREATE statement to create a specified table with appropriate columns and data types.
    load_source_data():
        Load the data that was pulled directly from Zoom, prior to transformation, into a Snowflake stage.
    """

    def __init__(self, data_config):
        """
        Constructs all the necessary attributes for the Load object.

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
        self.STAGE = data_config['load']['stage']

        # columns in dataframe that must be converted to datetime in Snowflake
        self.datetime_columns = data_config['load']['datetime_columns']

        # stage information
        self.days_to_stage = data_config['load']['days_to_stage']
        self.extract_path = data_config['extract']['download_path']

        # initialize logger
        self.log = Log('zoom_log', data_config)
        self.logger = self.log.get_logger()

    def load_table(self, df, table_name):
        """
        Loads Zoom data into a Snowflake table.

        Returns
        _______
        None
        """

        # connect to snowflake
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

        # create table if it doesn't exist, using create string method
        create_str = self.get_create_string(table_name, df, self.datetime_columns)
        cur.execute(create_str)

        # log table load
        self.logger.info(f'Loading {table_name}...')

        # write data to Snowflake
        success, num_chunks, num_rows, _ = write_pandas(conn, df, table_name, self.DATABASE, self.SCHEMA,
                                                        quote_identifiers=False)

        # log success status of table load
        if success:
            self.logger.info(f'{num_rows} rows successfully loaded into {table_name} table')
        else:
            self.logger.error(f'{table_name} table load unsuccessful')

        # close Snowflake connector
        conn.close()

    def get_create_string(self, table_name, df, datetime_columns):
        """
        Produces a SQL CREATE statement to create a specified table with appropriate columns and data types.

        Parameters
        __________
        table_name : str
            the name of the Snowflake table where the data will be held
        df : Pandas DataFrame
            dataframe that holds data to be loaded into Snowflake
        datetime_columns : list
            list of columnsthat must be converted to datetime upon loading into Snowflake

        Returns
        _______
        create_str : str
            SQL string for creating a Snowflake table that fits the dataframe
        """

        # dictionary of type conversions from pandas to snowflake
        dtype_conversions = {'object': 'string',
                             'int64': 'integer',
                             'Int64': 'integer',
                             'datetime64[ns]': 'TIMESTAMP_NTZ(9))',
                             'float64': 'REAL',
                             'boolean': 'BOOLEAN',
                             'category': 'string'}

        # get dictionary of data types of df for each column
        # here, "apply" allows us to get the data types as strings
        dtypes = df.dtypes.apply(lambda x: x.name).to_dict()

        # replace pandas types with snowflake types using conversion dictionary
        dtypes = {col: dtype_conversions[dtype] for col, dtype in dtypes.items()}

        # we have a few datetime columns that are technically string type
        # we want to convert these to TIMESTAMP_NTZ(9) upon loading, so we will alter the dictionary so this happens
        for col in datetime_columns:
            dtypes[col.upper().replace(' ', '_')] = 'TIMESTAMP_NTZ(9)'

        # make a create statement string form the dtypes dictionary
        create_str = f'CREATE TABLE IF NOT EXISTS {table_name}(' + \
                     ', '.join([f'"{key}" {value}' for key, value in dtypes.items()]) + ')'

        return create_str

    def load_source_data(self):
        """
        Load the data that was pulled directly from Zoom, prior to transformation, into a Snowflake stage.

        Returns
        _______
        None
        """

        # today's date
        today = datetime.now().strftime('%Y-%m-%d')  # Year-Month-Day format

        # connect to Snowflake
        conn = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT
        )

        # get Snowflake cursor
        cur = conn.cursor()

        # specify warehouse and database to use
        cur.execute(f'USE WAREHOUSE {self.WAREHOUSE}')
        cur.execute(f'USE DATABASE {self.DATABASE}')
        cur.execute(f'USE SCHEMA {self.SCHEMA}')

        # create stage if it doesn't exist
        cur.execute(
            f"CREATE STAGE IF NOT EXISTS {self.STAGE}")

        # put file in stage with today's date in the path
        cur.execute(f"PUT 'file://{self.extract_path}' @{self.STAGE}/{today}/call_log")

        # find files that are older than the specified number of days of data to keep
        cur.execute(f'list @{self.STAGE}')
        fetch = cur.fetchall()
        file_df = pd.DataFrame(fetch)
        # get dates from file paths as series
        dates = pd.to_datetime(file_df[0].str.split('/').str[1])
        # subtract days_to_keep days from today
        n_days = (pd.to_datetime(today) - timedelta(days=self.days_to_stage)).strftime('%Y-%m-%d')
        # find all dates in our series that are at least days_to_stage days old
        old_dates = dates[dates <= n_days].astype(str).unique().tolist()

        # remove all old files
        for date in old_dates:
            conn.cursor().execute(f'remove @{self.STAGE}/{date}')

        # close connector
        conn.close()
