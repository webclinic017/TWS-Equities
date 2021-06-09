# -*- coding: utf-8 -*-

from alive_progress import alive_bar

from tws_equities.helpers import isfile
from tws_equities.helpers import join

from tws_equities.helpers import create_batches
from tws_equities.helpers import delete_file
from tws_equities.helpers import get_files_by_type
from tws_equities.helpers import make_dirs
from tws_equities.helpers import sep
from tws_equities.helpers import save_data_as_json
from tws_equities.helpers import write_to_console
# from tws_equities.helpers import get_logger

from tws_equities.helpers import HISTORICAL_DATA_STORAGE as _HISTORICAL_DATA_STORAGE

from tws_equities.tws_clients.base import TWSWrapper
from tws_equities.tws_clients.base import TWSClient
from tws_equities.tws_clients.data_extractor import HistoricalDataExtractor

from tws_equities.settings import CACHE_DIR

from logging import getLogger

# TODO: add info messages to console
# TODO: handle duplicate file creation
# TODO: re-use cached input tickers
_CACHE_THRESHOLD = 10
_BATCH_SIZE = 30
_BAR_CONFIG = {
                    'title': '=> Status∶',
                    'calibrate': 5,
                    'force_tty': True,
                    'spinner': 'dots_reverse',
                    'bar': 'smooth'
              }
logger = getLogger(__name__)


def _cache_data(data, cache_success, cache_failure):
    for ticker in data:
        data_to_save = data[ticker]
        status = data_to_save['meta_data']['status']

        file_path = join(cache_success if status else cache_failure, f'{ticker}.json')
        save_data_as_json(data_to_save, file_path)


def _get_ticker_id(file_name):
    return int(file_name.split(sep)[-1].split('.')[0])


def _get_unprocessed_tickers(tickers, success_directory):
    """
        Exclude tickers that have already been processed successfully.
    """
    return list(set(tickers).difference(map(_get_ticker_id, get_files_by_type(success_directory))))


def _prep_for_extraction(tickers, end_date, end_time):
    """
        # todo: to be added...
    """
    # form data caching directory
    cache_directory = join(CACHE_DIR, end_date, end_time.replace(':', '_'))

    # create cache directory for success
    cache_success = join(CACHE_DIR, end_date, end_time.replace(':', '_'), 'success')
    make_dirs(cache_success)

    # create cache directory for failure
    cache_failure = join(CACHE_DIR, end_date, end_time.replace(':', '_'), 'failure')
    make_dirs(cache_failure)

    # save tickers for later use
    path_input_tickers = join(cache_directory, 'input_tickers.json')

    # todo: find a better way to do this
    if not isfile(path_input_tickers):
        save_data_as_json(tickers, path_input_tickers, indent=1, sort_keys=True)

    # extract tickers that are yet to be processed
    tickers = _get_unprocessed_tickers(tickers, cache_success)

    # clean failure directory, all these tickers will have to be processed again
    failure_tickers = list(map(_get_ticker_id, get_files_by_type(cache_failure)))
    common_tickers = list(set(tickers).intersection(failure_tickers))
    for ticker in common_tickers:
        file_name = f'{ticker}.json'
        delete_file(cache_failure, file_name)

    return tickers, cache_success, cache_failure


def extractor(tickers, end_date, end_time='15:01:00', duration='1 D', bar_size='1 min', what_to_show='TRADES',
              use_rth=0, date_format=1, keep_upto_date=False, chart_options=()):
    client = HistoricalDataExtractor(end_date=end_date, end_time=end_time, duration=duration,
                                     bar_size=bar_size, what_to_show=what_to_show, use_rth=use_rth,
                                     date_format=date_format, keep_upto_date=keep_upto_date,
                                     chart_options=chart_options, max_attempts=1, logger=logger)
    client.extract_historical_data(tickers)
    return client.data


def _run_extractor(batches, end_date, end_time, duration, bar_size, what_to_show, use_rth, date_format,
                   keep_upto_date, chart_options, cache_success, cache_failure, bar_title=None):
    # TODO: return tickers instead of files
    if bar_title is not None:
        _BAR_CONFIG['title'] = bar_title

    data, total = {}, len(batches)
    logger.debug(f'Batch-wise extraction initiated, total batches: {total}')
    with alive_bar(total=total, **_BAR_CONFIG) as bar:
        for i in range(total):
            batch = batches[i]
            # hold extracted data for the current batch in a temp variable
            # temp is a dictionary containing bar data for all tickers
            temp = extractor(batch, end_date, end_time, duration, bar_size, what_to_show,
                             use_rth, date_format, keep_upto_date, chart_options)

            # add temp to main data container
            data.update(temp)

            # cache data if current iteration is last or multiple of CACHE_THRESHOLD
            time_to_cache = (i+1 == total) or ((i > 0) and (i % _CACHE_THRESHOLD == 0))
            if time_to_cache:
                if bool(data):
                    _cache_data(data, cache_success, cache_failure)
                    logger.debug(f'Cached data for batch: {i+1}')
                    data = {}
            bar()  # update progress bar
    # return success & failure files
    return get_files_by_type(cache_success), get_files_by_type(cache_failure)


# noinspection PyUnusedLocal
def _cleanup(success_files, success_directory, failure_files, failure_directory, verbose=False):
    message = 'Post-extraction cleanup initiated...'
    write_to_console(message, verbose=verbose)

    # delete duplicate files
    # TODO: this operation should not be required
    duplicate_files = list(set(success_files).intersection(failure_files))
    for file in duplicate_files:
        delete_file(failure_directory, file)
    message = f'Cleaned {len(duplicate_files)} duplicate files...'
    write_to_console(message, pointer='->', indent=1, verbose=verbose)


# noinspection PyUnusedLocal
def _sanity_check(tickers, success_files, success_directory, failure_files, failure_directory):
    """
        TODO: To be implemented...initiate feedback loop from here
    """
    status = False
    return status


def extract_historical_data(tickers=None, end_date=None, end_time=None, duration='1 D',
                            bar_size='1 min', what_to_show='TRADES', use_rth=0, date_format=1,
                            keep_upto_date=False, chart_options=(), batch_size=_BATCH_SIZE,
                            max_attempts=3, run_counter=1, verbose=False):
    """
        A wrapper function around HistoricalDataExtractor, that pulls data from TWS for the given tickers.
        :param tickers: ticker ID (ex: 1301)
        :param end_date: end date (ex: '20210101')
        :param end_time: end time (ex: '15:00:01')
        :param duration: the amount of time to go back from end_date_time (ex: '1 D')
        :param bar_size: valid bar size or granularity of data (ex: '1 min')
        :param what_to_show: the type of data to retrieve (ex: 'TRADES')
        :param use_rth: 0 means retrieve data withing regular trading hours, else 0
        :param date_format: format for bar data, 1 means yyyyMMdd, 0 means epoch time
        :param keep_upto_date: setting to True will continue to return unfinished bar data
        :param chart_options: to be documented
        :param batch_size: size of each batch as integer, default=30
        :param max_attempts: maximum number of times to try for failure tickers
        :param run_counter: counts the number of attempts performed, not to be used from outside
        :param verbose: set to True to display messages on console
    """
    logger.info(f'Running extractor, attempt: {run_counter} | max attempts: {max_attempts}')
    # let the user know that data extraction has been initiated
    if run_counter == 1:
        _date_formatted = f'{end_date[:4]}/{end_date[4:6]}/{end_date[6:]}'
        message = f'{"-" * 30} Data Extraction: {_date_formatted} {"-" * 30}'
        write_to_console(message, verbose=True)

    # additional info, if user asks for it
    message = f'Setting things up for data-extraction...'
    write_to_console(message, indent=2, verbose=verbose)
    tickers, cache_success, cache_failure = _prep_for_extraction(tickers, end_date, end_time)
    write_to_console('Refreshed cache directories...', indent=4, pointer='->', verbose=verbose)
    write_to_console('Removed already cached tickers...', indent=4, pointer='->', verbose=verbose)
    write_to_console('Reset failed tickers...', indent=4, pointer='->', verbose=verbose)

    write_to_console('Generating ticker batches...', indent=2, verbose=verbose)
    batches = create_batches(tickers, batch_size)
    write_to_console(f'Total Tickers: {len(tickers)}', indent=4, verbose=verbose, pointer='->')
    write_to_console(f'Total Batches: {len(batches)}', indent=4, verbose=verbose, pointer='->')
    write_to_console(f'Batch Size: {batch_size}', indent=4, verbose=verbose, pointer='->')

    # core processing section
    bar_title = f'=> Attempt: {run_counter}'
    message = 'Batch-wise extraction in progress, this can take some time. Please be patient...'
    write_to_console(message, indent=2, verbose=verbose)
    success_files, failure_files = _run_extractor(batches, end_date, end_time, duration, bar_size,
                                                  what_to_show, use_rth, date_format, keep_upto_date,
                                                  chart_options, cache_success, cache_failure,
                                                  bar_title=bar_title)

    run_counter += 1
    # feedback loop, process failed or missing tickers until we hit the max attempt threshold
    if tickers != list(map(_get_ticker_id, success_files)):
        if run_counter <= max_attempts:
            # TODO: optimize
            unprocessed_tickers = set(tickers).difference(map(_get_ticker_id, success_files))
            batch_size = 10
            extract_historical_data(tickers=unprocessed_tickers, end_date=end_date, end_time=end_time,
                                    duration=duration, bar_size=bar_size, what_to_show=what_to_show,
                                    use_rth=use_rth, date_format=date_format, keep_upto_date=keep_upto_date,
                                    chart_options=chart_options, batch_size=batch_size,
                                    run_counter=run_counter)
        _cleanup(success_files, cache_success, failure_files, cache_failure, verbose=verbose)


if __name__ == '__main__':
    # NOTE: View results at: TWS-Equities/historical_data/<end_date>/.success/<ticker_id>.json
    target_tickers = [1301]
    extract_historical_data(target_tickers,
                            end_date='20210101',
                            end_time='15:01:00',
                            duration='1 D',
                            batch_size=5)
