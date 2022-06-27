import json
import sys
import os

from Extract import Extract
from Load import Load
from Transform import Transform
from Log import Log


# load json file of ETL specifications
data_config = json.load(open('data_config.json'))

# create logger
log = Log('zoom_log', data_config)
logger = log.get_logger()

# when an exception occurs, we will handle it with the log function
sys.excepthook = log.exception_logging

# initialize extract object and extract data
logger.info('Begin extract data...')
e = Extract(data_config)
call_df = e.extract_data(use_date_range=False)
logger.info('Data sucessfully extracted. Begin downloading extracted data...')

# download extracted data
e.download_extracted_data()
logger.info('Extracted data successfully downloaded. Begin transform data...')

# initialize tranform object and transform data
t = Transform(data_config)
call_df = t.transform_data(call_df)
logger.info('Data sucessfully transformed. Begin load data into Snowflake...')

# initialize load object and load call data into Snowflake
l = Load(data_config)
l.load_table(call_df, 'Zoom_Call_Logs')
l.load_source_data()
logger.info('Data sucessfully loaded into Snowflake.')

# create ZOOM_LOG table in Snowflake
# this only needs to be ran once, or if the table has been removed
# log.create_log_table(l)

# load log file into Snowflake
log.load_log()

# close log
log.close_log()

# delete local log file
os.remove('./ETLLog.log')
