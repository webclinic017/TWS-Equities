#! TWS-Project/venv/bin/python3.9
# -*- coding: utf-8 -*-

"""
    Historical data extractor, written around TWS API(reqHistoricalData)
"""

from tws_equities.tws_clients import TWSWrapper
from tws_equities.tws_clients import TWSClient
from tws_equities.helpers import create_stock
from tws_equities.helpers import make_dirs
from logging import getLogger
from os import name as os_name
import signal


OS_IS_UNIX = os_name == 'posix'


class HistoricalDataExtractor(TWSWrapper, TWSClient):

    def __init__(self, end_date='20210101', end_time='15:01:00', duration='1 D', bar_size='1 min',
                 what_to_show='TRADES', use_rth=0, date_format=1, keep_upto_date=False, chart_options=(),
                 logger=None, timeout=3, max_attempts=3):
        TWSWrapper.__init__(self)
        TWSClient.__init__(self, wrapper=self)
        self.ticker = None
        self._target_tickers = []
        self._processed_tickers = []
        self.is_connected = False
        self.directory_maker = make_dirs
        # TODO: implement broken connection handler
        self.connection_is_broken = False
        self.handshake_completed = False
        self.end_date = end_date
        self.end_time = end_time
        self.duration = duration
        self.bar_size = bar_size
        self.what_to_show = what_to_show
        self.use_rth = use_rth
        self.date_format = date_format
        self.keep_upto_date = keep_upto_date
        self.chart_options = chart_options
        self.timeout = timeout
        self.logger = logger or getLogger(__name__)
        self.max_attempts = max_attempts
        self.data = None

    def _init_data_tracker(self, ticker):
        """
            Initializes the data tracker.
            Should be invoked for every new ticker.
        """
        _meta_data = {'start': None, 'end': None, 'status': False, 'attempts': 0,
                      '_error_stack': [], 'total_bars': 0, 'ecode': ticker}
        _initial_data = {'meta_data': _meta_data, 'bar_data': []}
        self.data[ticker] = _initial_data
        self.logger.info(f'Initialized data tracker for ticker: {ticker}')

    def _reset_attr(self, **kwargs):
        """
            Resets the value of the given attributes
            :param kwargs: keyword argument to reset the attribute
        """
        for attr, value in kwargs.items():
            if value is None:
                raise ValueError(f'Attribute {attr} can not be None.')
            setattr(self, attr, value)
            self.logger.debug(f'Attribute: {attr} was reset to: {value}')
        self.logger.info('Extractor client object was reset successfully')

    def _set_timeout(self, ticker):
        """
            Break the call running longer than timeout threshold.
            Call error method with code=-1 on timeout.
            NOTE: Not supported on Windows OS yet.
        """
        # noinspection PyUnusedLocal
        def _handle_timeout(signum, frame):
            try:
                _message = f'Data request for ticker: {ticker} timed out after: {self.timeout} seconds'
                self.error(ticker, -1, _message)
            except OSError as e:  # TODO: make it work for Windows OS
                self.logger.error(f'{e}')

        # TODO: final alarm
        # if OS_IS_UNIX:
        signal.signal(signal.SIGALRM, _handle_timeout)
        signal.alarm(self.timeout)

    def _request_historical_data(self, ticker):
        """
            Sends request to TWS API
        """
        try:
            if OS_IS_UNIX:
                self._set_timeout(ticker)
            contract = create_stock(ticker)
            end_date_time = f'{self.end_date} {self.end_time}'
            self.logger.info(f'Requesting historical data for ticker: {ticker}')
            self.data[ticker]['meta_data']['attempts'] += 1
            self.reqHistoricalData(ticker, contract, end_date_time, self.duration, self.bar_size,
                                   self.what_to_show, self.use_rth, self.date_format, self.keep_upto_date,
                                   self.chart_options)
        except OSError as e:  # TODO: temporary piece, find a fix for Windows
            self.logger.critical(f'{e}')

    def _extraction_check(self, ticker):
        """
            Returns True if:
                - Bar data has been extracted and saved to success location
                                    OR
                - Error data has been extracted and saved to failure location
        """
        self.logger.info(f'Checking extraction status for ticker: {ticker}')
        meta_data = self.data[ticker]['meta_data']
        status = meta_data['status']
        max_attempts_reached = meta_data['attempts'] >= self.max_attempts
        return_value = status or max_attempts_reached
        self.logger.debug(f'Extraction status check result for ticker: {ticker} is "{return_value}"')
        return return_value

    def connect(self, host='127.0.0.1', port=7497, client_id=10):
        """
            Establishes a connection to TWS API
            Sets 'is_connected' to True after a successful connection
            @param host: IP Address of the machine hosting the TWS app
            @param port: Port number on which TWS is listening to new connections
            @param client_id: Client ID using which connection will be made
        """
        self.logger.info('Trying to connect to TWS API server')
        if not self.is_connected:
            super().connect(host, port, client_id)
            self.is_connected = self.isConnected()
            self.logger.debug(f'Connection status: {self.is_connected}')
            self.run()

    def disconnect(self):
        """
            Disconnects the client from TWS API
        """
        self.logger.info('Extraction completed, terminating main loop')
        self._reset_attr(is_connected=False, handshake_completed=False)
        super().disconnect()

    def run(self):
        """
            Triggers the infinite message loop defined within parent class(EClient):
                - Completes the initial handshake
                - Triggers data extraction from error method
            Note: User must connect to TWS API prior to calling this method.
        """
        self.logger.info('Initiating main loop')
        if not self.is_connected:
            raise ConnectionError(f'Not connected to TWS API, please launch TWS and enable API settings.')
        super().run()

    def extract_historical_data(self, tickers=None):
        """
            Performs historical data extraction on tickers provided as input.
        """
        if tickers is not None:
            self._reset_attr(_target_tickers=tickers, data={})
        if not self.is_connected:
            self.connect()
        unprocessed_tickers = list(set(self._target_tickers).difference(self._processed_tickers))
        if bool(unprocessed_tickers):
            self.logger.info(f'Found unprocessed tickers, proceeding with data extraction')
            _target_ticker = unprocessed_tickers[0]
            self.logger.debug(f'Target ticker: {_target_ticker}')
            if _target_ticker not in self.data:
                self._init_data_tracker(_target_ticker)
            ticker_is_not_processed = not self._extraction_check(_target_ticker)
            if ticker_is_not_processed:
                self._request_historical_data(_target_ticker)
            else:
                self._processed_tickers.append(_target_ticker)
                self.logger.debug(f'Ticker was processed, completion marked')
                self.extract_historical_data()
        else:
            self.disconnect()

    def historicalData(self, ticker, bar):
        """
            This method is receives data from TWS API, invoked automatically after "reqHistoricalData".
            :param ticker: represents ticker ID
            :param bar: a bar object that contains OHLCV data
        """
        self.logger.info(f'Bar-data received for ticker: {ticker}')
        time_stamp = bar.date
        date, time = time_stamp.split()
        year, month, day = date[:4], date[4:6], date[6:]
        hour, minute, second = map(int, time.split(':'))
        time_stamp = f'{year}-{month}-{day} {time}'
        session = 1 if hour < 12 else 2
        bar = {'time_stamp': time_stamp, 'open': bar.open, 'high': bar.high, 'low': bar.low,
               'close': bar.close, 'volume': bar.volume, 'average': bar.average,
               'count': bar.barCount, 'session': session}
        if not((hour == 11 and minute > 30) or (hour == 12 and minute < 30)):  # fixme: temporary hack
            if bar not in self.data[ticker]['bar_data']:
                self.data[ticker]['bar_data'].append(bar)
        self.logger.debug(f'Ticker ID: {ticker} | Bar-data: {bar}')

    def historicalDataEnd(self, ticker, start, end):
        """
            This method is called automatically after all the bars have been generated by "historicalData".
            Marks the completion of historical data generation for a given ticker.
            :param ticker: ticker ID
            :param start: starting timestamp
            :param end: ending timestamp
        """
        self.logger.info(f'Data extraction completed for ticker: {ticker}')
        self.data[ticker]['meta_data']['start'] = start
        self.data[ticker]['meta_data']['end'] = end
        self.data[ticker]['meta_data']['status'] = True
        self.data[ticker]['meta_data']['total_bars'] = len(self.data[ticker]['bar_data'])
        self._processed_tickers.append(ticker)
        self.extract_historical_data()

    def error(self, ticker, code, message):
        """
            Error handler for all API calls, invoked directly by EClient methods
            :param ticker: error ID (-1 means no informational message, not true error)
            :param code: error code, defines error type
            :param message: error message, information about error
        """
        # -1 is not a true error, but only an informational message
        # initial call to run invokes this method with error ID = -1
        # print(f'Error: ID: {id} | Code: {code} | String: {message}')
        if ticker == -1:
            # error code 502 indicates connection failure
            if code == 502:
                self.logger.error(f'Connection Failure: {message}, Error Code: {code}')
                raise ConnectionError('Could not connect to TWS, please ensure TWS is running.')
            # error codes 2103, 2105, 2157 indicate broken connection
            if code in [2103, 2105, 2157]:
                self.logger.error(f'Insecure Connection: {message}, Error code: {code}')
                raise ConnectionError(f'Detected broken connection, please try re-connecting the web-farms '
                                      f'in TWS.')
            # error codes 2104, 2106, 2158 indicate connection is OK
            if code in [2104, 2106, 2158]:
                self.logger.debug(message)
            # last error code received, marks the completion of initial hand-shake
            # call back the extractor to start pulling historical data
            if code == 2158:
                self.logger.info(f'Secure connection established to TWS API.')
                self.handshake_completed = True
        else:
            self.logger.error(f'{message}: Ticker ID: {ticker}, Error Code: {code}')
            meta_data = self.data[ticker]['meta_data']
            attempts = meta_data['attempts']
            error = {'code': code, 'message': message}
            meta_data['_error_stack'].append(error)

            if attempts >= self.max_attempts:
                self._processed_tickers.append(ticker)

            # -1 indicates a timeout
            # 162 indicates that HMDS return no data
            # 322 indicates that API request limit(50) has been breached
            # 504 indicates no connection
            if code in [-1, 322, 504]:
                self.cancelHistoricalData(ticker)
                self.logger.error(f'Canceling: {ticker} | {code} | {message}')

        if self.handshake_completed:
            self.logger.debug('Initial handshake completed, initiating data extraction')
            extraction_is_not_completed = self._target_tickers != self._processed_tickers
            if extraction_is_not_completed:
                self.extract_historical_data()
            else:
                self.disconnect()


if __name__ == '__main__':
    import json
    target_tickers = [1301]
    extractor = HistoricalDataExtractor(end_date='20210210', end_time='09:01:00')
    extractor.extract_historical_data(target_tickers)
    print(json.dumps(extractor.data, indent=1, sort_keys=True))
