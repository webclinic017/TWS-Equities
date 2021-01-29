# -*- coding: utf-8 -*-


import pytest
from datetime import datetime as dt
from tws_equities.tws_clients import extractor


"""
    HistoricalDataExtractor is a TWS Client that extracts data from the API
    and stores it in a dictionary using input ticker ID(s) as keys.
    "extractor" method is a much easier to use wrapper around the same and
    this module tries to test it's functionality on high-level.

    Here's how you can trigger these tests:
        - Simple run: Trigger all tests
            - pytest tests/data_extractor_test.py
        - Parameterized Run: Trigger specific tests
            - pytest tests/data_extractor_test.py -m positive
            - pytest tests/data_extractor_test.py -m negative

    NOTE:
        - Tests are marked as positive and negative inside the module.
        - Custom grouping can be done by specifying new groups inside "pytest.ini".
"""


postive_test_tickers = [1301, 5386, 7203, 8729]
negative_test_tickers = [1743, 3437, 4628, 1234]

date_format = r'%Y%m%d'
end_date = dt.today().date().strftime(date_format)  # current date
end_time = '15:01:00'  # after market close

positive_data = extractor(postive_test_tickers, end_date, end_time=end_time)
negative_data = extractor(negative_test_tickers, end_date, end_time=end_time)


def validate_bar_data(data, ticker):
    bar_keys = ['average', 'close', 'count', 'high', 'low', 'open', 'session', 'time_stamp', 'volume']
    for bar in data:
        assert isinstance(bar, dict), f'Found invalid bar data for ticker: {ticker}'
        assert bar_keys == sorted(list(bar.keys())), f'Found invalid keys in bar data for ticker: {ticker}'
        average, close, count, high, low = bar['average'], bar['close'], bar['count'], bar['high'], bar['low']
        open_, session, time_stamp, volume = bar['open'], bar['session'], bar['time_stamp'], bar['volume']
        assert isinstance(average, float), 'Average value in bar is not a float.'
        assert isinstance(close, float), 'Close value in bar is not a float.'
        assert isinstance(count, int), 'Count value in bar is not a int.'
        assert isinstance(high, float), 'High value in bar is not a float.'
        assert isinstance(low, float), 'Low value in bar is not a float.'
        assert isinstance(open_, float), 'Open value in bar is not a float.'
        assert isinstance(session, int), 'Session value in bar is not a int.'
        assert session in [1, 2], 'Session value is invalid'
        assert isinstance(time_stamp, str), 'Timestamp value in bar is not a str.'
        assert isinstance(volume, int), 'Volume value in bar is not a int.'


def validate_meta_data(data, ticker):
    meta_keys = ['_error_stack', 'attempts', 'ecode', 'end', 'start', 'status', 'total_bars']
    assert sorted(data.keys()) == meta_keys, 'Meta data has bad keys.'
    error_stack, attempts, ecode = data['_error_stack'], data['attempts'], data['ecode']
    end, start, status, total_bars = data['end'], data['start'], data['status'], data['total_bars']
    assert isinstance(error_stack, list), 'Error stack is not a list'
    assert isinstance(attempts, int), 'Attempts value is not int'
    assert 0 < attempts <= 3, 'Invalid value for number of attempts.'
    assert isinstance(ecode, int), 'Ecode is not an integer.'
    assert ecode == ticker, 'Ecode is not equal to given ticker ID'
    assert isinstance(status, bool), 'Status value is not a boolean.'
    assert isinstance(total_bars, int), 'Total bars value is not an integer.'

    if status:
        assert isinstance(end, str), 'End value is not a string.'
        assert isinstance(start, str), 'Start value is not a string.'

    if not status:
        error_keys = ['code', 'message']
        for error in error_stack:
            assert isinstance(error, dict), 'Error object is not a dictonary.'
            assert sorted(error.keys()) == error_keys, 'Invalid error keys.'


def validate_positive_data(input_tickers, extracted_data):
    assert isinstance(extracted_data, dict), 'Extracted data is not a dictionary.'
    assert all(ticker in input_tickers for ticker in extracted_data.keys()), 'Extracted data does not ' \
                                                                             'contain all the tickers.'
    for ticker, data in extracted_data.items():
        assert 'bar_data' in data, f'Bar data has not been extracted for ticker: {ticker}'
        assert 'meta_data' in data, f'Meta data not available for ticker: {ticker}'
        bar_data = data['bar_data']
        assert isinstance(bar_data, list), f'Bar data is not a list for ticker: {ticker}'
        validate_bar_data(bar_data, ticker)


def validate_negative_data(input_tickers, extracted_data):
    assert isinstance(extracted_data, dict), 'Extracted data is not a dictionary.'
    assert all(ticker in input_tickers for ticker in extracted_data.keys()), 'Extracted data does not ' \
                                                                             'contain all the tickers.'
    for ticker, data in extracted_data.items():
        assert 'bar_data' in data, f'Bar data has not been extracted for ticker: {ticker}'
        assert 'meta_data' in data, f'Meta data not available for ticker: {ticker}'
        meta_data = data['meta_data']
        assert isinstance(meta_data, dict), f'Meta data is not a dictionary for ticker: {ticker}'
        validate_meta_data(meta_data, ticker)


@pytest.mark.positive
def test_extractor_positive():
    validate_positive_data(postive_test_tickers, positive_data)


@pytest.mark.negative
def test_extractor_negative():
    validate_negative_data(negative_test_tickers, negative_data)
