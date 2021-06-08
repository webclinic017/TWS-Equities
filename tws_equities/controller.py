#!TWS-Project/venv/bin/python3.9
# -*- coding: utf-8 -*-


from tws_equities.data_files import create_csv_dump
# from tws_equities.data_files import generate_extraction_metrics
from tws_equities.helpers import get_date_range
from tws_equities.tws_clients import extract_historical_data


# TODO: use verbose and debug options


def download(tickers=None, start_date=None, end_date=None, end_time=None,
             duration=None, bar_size=None, what_to_show=None, use_rth=None, verbose=False):
    if start_date is None:
        start_date = end_date
    if end_date is None:
        raise ValueError(f'User must specify at least the end date for data extraction.')
    date_range = get_date_range(start_date, end_date)
    for date in date_range:
        extract_historical_data(tickers=tickers, end_date=date, end_time=end_time, duration=duration,
                                bar_size=bar_size, what_to_show=what_to_show, use_rth=use_rth,
                                verbose=verbose)


def convert(start_date=None, end_date=None, end_time='15:01:00', verbose=False):
    if start_date is None:
        start_date = end_date
    if end_date is None:
        raise ValueError(f'User must pass at least the end date for data conversion.')
    date_range = get_date_range(start_date, end_date)
    for date in date_range:
        create_csv_dump(date, end_time=end_time)


def metrics(tickers=None, start_date=None, end_date=None, end_time='15:01:00', verbose=False):
    if start_date is None:
        start_date = end_date
    if end_date is None:
        raise ValueError(f'User must pass at least the end date for metrics generation.')
    if tickers is None:
        pass  # fixme: read cached input
    date_range = get_date_range(start_date, end_date)
    # for date in date_range:
    #     generate_extraction_metrics(date, end_time=end_time, input_tickers=tickers)


def run(tickers=None, start_date=None, end_date=None, end_time=None, duration='1 D',
        bar_size='1 min', what_to_show='TRADES', use_rth=1, verbose=False, debug=False):
    download(tickers=tickers, start_date=start_date, end_date=end_date, end_time=end_time,
             duration=duration, bar_size=bar_size, what_to_show=what_to_show, use_rth=use_rth,
             verbose=verbose)
    convert(start_date=start_date, end_date=end_date, end_time=end_time)
    metrics(tickers=tickers, start_date=start_date, end_date=end_date, end_time=end_time)
