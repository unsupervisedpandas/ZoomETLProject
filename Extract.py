import jwt
from requests import Session
from time import time
import pandas as pd
from ratelimiter import RateLimiter
from datetime import datetime
from dateutil.relativedelta import relativedelta

from Log import Log


class Extract:
    """
    A class to extract data from Zoom call logs.

    ...

    Methods
    _______
    extract_data():
        Extracts data from Zoom through the API.
    get_date_range():
        Gets start and end date to pull data from.
    get_token():
        Generates a request token from the API key and secret using the pyjwt library.
    is_positive():
        Takes in an integer n, returns 1 if the number is positive, else returns 0.
    limited_request_get():
        Makes http requests without exceeding the max number of requests per second (5) for the api.
    def download_extracted_data(self):
        Downloads extracted data as csv
    """
    def __init__(self, data_config):
        """
        Constructs all the necessary attributes for the extract object.

        Parameters
        __________
            data_config : dict
                dictionary containing ETL specifications
            use_date_range : bool
                specifies whether to extract data from a date range. If false, use the period paramater in the
                data_config to pull the day, month, or year of data up until the start date.
        """

        # get parameters from data_config
        self.API_KEY = data_config['extract']['API_KEY']
        self.API_SECRET = data_config['extract']['API_SECRET']
        self.page_size = data_config['extract']['page_size']
        self.period = data_config['extract']['period']    # get day, month, or year
        # number of days, months, or years (1 corresponds to the current month)
        self.num_periods = data_config['extract']['num_periods']
        self.start_date = data_config['extract']['start_date']
        self.end_date = data_config['extract']['end_date']
        self.download_path = data_config['extract']['download_path']

        # untransformed data
        self.extracted_data = pd.DataFrame()

        # initialize logger
        self.log = Log('zoom_log', data_config)
        self.logger = self.log.get_logger()

    def extract_data(self, use_date_range):
        """
        Extracts data from Zoom through the API.

        Parameters
        __________
        use_date_range : bool
            specifies whether to extract data from a date range. If false, use the period paramater in the
            data_config to pull the day, month, or year of data up until the start date.

        Returns
        _______
        df : Pandas DataFrame
            dataframe containing call records pulled from Zoom
        """

        # get date range to pull data from
        start_date, end_date = self.get_date_range(use_date_range)

        # pull call logs from Zoom API into a pandas dataframe
        df = self.get_call_logs(start_date, end_date, self.page_size)

        # log some summary stats about the extracted dataframe
        self.logger.info(
            f"The extracted dataframe has max datetime of {df['date_time'].max()},"
            f"min datetime of {df['date_time'].min()},"
            f"{df.shape[0]} rows,"
            f"and {df.shape[1]} columns.")

        return df

    def get_date_range(self, use_date_range):
        """
        Gets start and end date to pull data from.

        Parameters
        __________
        use_date_range : bool
            specifies whether to extract data from a date range. If false, use the period paramater in the
            data_config to pull the day, month, or year of data up until the start date.

        Returns
        _______
        start_date : str
            start date of date range to pull data from
        end_date : str
            end date of date range to pull data from
        """

        period = self.period
        num_periods = self.num_periods
        end_date = self.end_date

        # if end date is today, get today's date
        if end_date == 'today':
            end_date = datetime.today().strftime('%Y-%m-%d')

        # if we are not using a specified date range, calculate the start date from the period and num_periods
        # convert start date to string type
        if not use_date_range:
            # today's date
            today = datetime.today().strftime('%Y-%m-%d')
            if period == 'day':
                # get only the date as a datetime object
                start_date = datetime.strptime(today, '%Y-%m-%d')
                # subtract days according to the number of periods
                start_date = start_date - relativedelta(days=(num_periods - 1))
                # convert to string type
                start_date = start_date.strftime('%Y-%m-%d')
            elif period == 'month':
                # get only the date as a datetime object
                start_date = datetime.strptime(today, '%Y-%m-%d')
                # subtract months according to the number of periods
                start_date = start_date - relativedelta(months=(num_periods - 1))
                # change the day to the 1st of the month and convert to string type
                start_date = start_date.replace(day=1).strftime('%Y-%m-%d')
            elif period == 'year':
                start_date = datetime.strptime(today, '%Y-%m-%d')
                start_date = start_date - relativedelta(years=(num_periods - 1))
                start_date = start_date.replace(month=1, day=1).strftime('%Y-%m-%d')
            else:
                raise ValueError('Invalid value for argument period in data_config.')
        else:
            start_date = self.start_date

        return start_date, end_date

    def generate_token(self):
        """
        Generates a request token from the API key and secret using the pyjwt library.

        Returns
        _______
        token : str
            request token for accessing the Zoom Api
        """
        token = jwt.encode(
            # create a payload of the token containing API Key & expiration time
            {'iss': self.API_KEY, 'exp': time() + 5000},
            # secret used to generate token signature
            self.API_SECRET,
            # specify the hashing alg
            algorithm='HS256'
        )
        return token

    def is_positive(self, n):
        """Takes in an integer n, returns 1 if the number is positive, else returns 0."""
        if n > 0:
            return 1
        else:
            return 0

    @RateLimiter(max_calls=5, period=1)
    def limited_request_get(self, session, url, headers):
        """
        Makes http requests without exceeding the max number of requests per second (5) for the api.

        Parameters
        __________
        session : Session object
            requests Session object for making http requests
        url : str
            the url to request
        headers : dict
            header for authorization and specifying content type in http request

        Returns
        _______
        req : Response object
            reponse object from the API request
        """
        req = session.get(url, headers=headers)
        return req

    def get_call_logs(self, start_date, end_date, page_size):
        """
        Makes http requests to get call log data from Zoom API. Calculates how many pages to request based on number
        of call records.

        Parameters
        __________
        start_date : str
            start date to pull data from
        end_date : str
            end date to pull data from
        page_size : int
            number of records to pull per page (max 300)

        Returns
        _______
        call_df : Pandas DataFrame
            dataframe containing call records pulled from Zoom
        """

        # construct request header with jwt token
        header = {'authorization': 'Bearer %s' % self.generate_token(),
                  'content-type': 'application/json'}

        # ensure page_size does not exceed 300
        if page_size > 300:
            raise ValueError(f'page_size value of {page_size} exceeds that maximum value of 300.')

        # create initial request url
        url = f'https://api.zoom.us/v2/phone/call_logs?page_size={page_size}&from={start_date}&to={end_date}'

        # initalize Session object
        session = Session()

        # initialize the number of pages, a list to hold dictionaries obtained from responses, and a counter
        pages = 0
        log_dicts = []
        counter = 0

        # submit requests, updating the url with a next_page_token each time, until the number of pages equals the
        # counter
        while True:
            # submit initial request
            response = self.limited_request_get(session, url, header)

            # increment counter
            counter += 1

            # get call data as a dictionary
            log_dict = response.json()

            # on our first loop, calculate number of pages
            if counter == 1:
                records = log_dict['total_records']
                # log number of records available, start date, and end date
                self.logger.info(f'{records} records available between {start_date} and {end_date}')
                # if there were no calls, stop
                if records == 0:
                    self.logger.error(f'No records available between {start_date} and {end_date}')
                    break

                # calculate the number of pages to request
                # if there are any left over calls that did not make up a full page, we will add a page for them
                pages = records // page_size + self.is_positive(records % page_size)

            # append dictionary to list of log_dicts
            log_dicts.append(log_dict)

            # when we reach the desired number of pages, stop
            # if we don't stop, it will loop back to the beginning (within date requirements)
            if pages == counter:
                break

            # update url with next_page_token parameter
            next_page_token = log_dict['next_page_token']
            url = f'https://api.zoom.us/v2/phone/call_logs?page_size=' \
                  f'{page_size}&next_page_token={next_page_token}&from={start_date}&to={end_date}'

        # get only call logs from reponse dictionaries in log_dicts
        call_list = list(map(lambda d: d['call_logs'], log_dicts))

        # convert list of lists of dictionaries to a list of dictionaries
        call_list = [call_dict for sublist in call_list for call_dict in sublist]

        # convert to a pandas dataframe
        call_df = pd.DataFrame(call_list)

        self.extracted_data = call_df

        return call_df

    def download_extracted_data(self):
        """
        Downloads extracted data as csv

        Returns
        _______
        None
        """

        self.extracted_data.to_csv(self.download_path)




