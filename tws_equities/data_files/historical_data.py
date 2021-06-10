# -*- coding: utf-8 -*-


from alive_progress import alive_bar
import pandas as pd
from logging import getLogger

from tws_equities.data_files import get_japan_indices
from tws_equities.helpers import read_json_file
from tws_equities.helpers import save_data_as_json
from tws_equities.helpers import make_dirs
from tws_equities.helpers import delete_directory
from tws_equities.helpers import get_files_by_type
from tws_equities.helpers import read_csv
from tws_equities.helpers import isfile
from tws_equities.helpers import isdir
from tws_equities.helpers import join
from tws_equities.helpers import sep
from tws_equities.helpers import glob
from tws_equities.helpers import write_to_console

from tws_equities.settings import CACHE_DIR
from tws_equities.settings import HISTORICAL_DATA_STORAGE
from tws_equities.settings import MONTH_MAP
from tws_equities.settings import DAILY_METRICS_FILE
from tws_equities.settings import GREEN_TICK
from tws_equities.settings import RED_CROSS


_BAR_CONFIG = {
                    'title': '=> Status∶',
                    'calibrate': 5,
                    'force_tty': True,
                    'spinner': 'dots_reverse',
                    'bar': 'smooth'
              }
logger = getLogger(__name__)


def _get_marker(ratio, threshold=0.1):
    return GREEN_TICK if ratio >= threshold else RED_CROSS


def _setup_storage_directories(date, bar_size='1 min'):
    y, m = date[:4], date[4:6]
    storage_dir = join(HISTORICAL_DATA_STORAGE, bar_size.replace(' ', ''),
                       y, MONTH_MAP[int(m)], date)

    make_dirs(storage_dir)
    return storage_dir


# TODO: both dataframe generators could be refactored into a generic fucntion.
# fixme: account for empty dataframes
def generate_success_dataframe(target_directory, bar_title=None, verbose=False):
    """
        Creates a pandas datafame from JSON files present at target_directory.
        Assumes that all these JSON files have valid bar data.

        Parameters:
        -----------
        target_directory(str): location to read JSON files from
        bar_title(str): message to show infront of the progress bar
        verbose(bool): set to true to see info messages on console
    """
    if bar_title is not None:
        _BAR_CONFIG['title'] = bar_title

    def _get_ticker_id(file_name):
        return int(file_name.split(sep)[-1].split('.')[0])

    # create a place holder dataframe
    expected_columns = ['time_stamp', 'ecode', 'session', 'open', 'high', 'low',
                        'close', 'volume', 'average', 'count']

    # create temporary directory to store smaller CSV files
    temp_directory = '.temp'
    make_dirs(temp_directory)

    # extract all json files from target directory
    success_files = get_files_by_type(target_directory)
    success_tickers = list(map(_get_ticker_id, success_files))
    total = len(success_tickers)
    data = pd.DataFrame(columns=expected_columns)

    if bool(total):
        write_to_console(f'=> Generating dataframe for success tickers...', verbose=verbose)
        json_generator = map(read_json_file, success_files)
        counter = 0  # to count temp files
        with alive_bar(total=total, **_BAR_CONFIG) as bar:
            for i in range(total):
                ticker = success_tickers[i]
                ticker_data = next(json_generator)  # load data into a dictionary
                bar_data, meta_data = ticker_data['bar_data'], ticker_data['meta_data']
                temp_data = pd.DataFrame(bar_data)
                temp_data['ecode'] = ticker
                data = data.append(temp_data)
                _time_to_cache = ((i > 0) and (i % 100 == 0)) or (i+1 == total)
                if _time_to_cache:
                    if data.shape[0] > 0:
                        temp_file = join(temp_directory, f'success_{counter}.csv')
                        data.to_csv(temp_file)
                        data = pd.DataFrame(columns=expected_columns)
                        counter += 1
                bar()

        # merge all CSV files into a single dataframe
        # delete all temp files
        temp_files = get_files_by_type(temp_directory, file_type='csv')
        if bool(temp_files):
            data = pd.concat(map(read_csv, temp_files))
            data.sort_values(by=['ecode', 'time_stamp'], inplace=True, ignore_index=True)
            data = data[expected_columns]
    delete_directory(temp_directory)

    return data


def generate_failure_dataframe(target_directory, bar_title=None, verbose=False):
    """
        Creates a pandas data fame from JSON files present at the given failure location.
        Assumes that all these JSON files have valid error stacks.
        :param target_directory: location to read JSON files from
        :param bar_title: message to show infron of progress bar
        :param verbose: set to true to see info messages on console
    """
    if bar_title is not None:
        _BAR_CONFIG['title'] = bar_title

    def _get_ticker_id(file_name):
        return int(file_name.split(sep)[-1].split('.')[0])

    # create a place holder dataframe
    expected_columns = ['ecode', 'code', 'message']
    data = pd.DataFrame(columns=expected_columns)

    # create temporary directory to store smaller CSV files
    temp_directory = '.temp'
    make_dirs(temp_directory)

    # extract all json files from target directory
    file_pattern = join(target_directory, '*.json')  # TODO: can be modified to match digital values
    failure_files = glob(file_pattern)
    total = len(failure_files)

    if bool(total):
        write_to_console(f'=> Generting dataframe for failure tickers...', verbose=verbose)
        json_generator = map(read_json_file, failure_files)
        counter = 0  # to count temp CSV files
        with alive_bar(total=total, **_BAR_CONFIG) as bar:
            for i in range(total):
                ticker_data = next(json_generator)
                meta = ticker_data['meta_data']
                error_stack = meta['_error_stack']
                ecode = meta.get('ecode', _get_ticker_id(failure_files[i]))
                temp_data = pd.DataFrame(error_stack, columns=expected_columns)
                temp_data['ecode'] = ecode
                # if error stack is empty, then create a dummy row
                if temp_data.shape[0] == 0:  # fixme: find a way to control this in the TWS Client
                    dummy_row = {'ecode': ecode, 'code': 'unknown', 'message': 'not available'}
                    temp_data = temp_data.append(dummy_row, ignore_index=True)

                data = data.append(temp_data)
                _time_to_cache = (i+1 == total) or ((i > 0) and (i % 100 == 0))
                if _time_to_cache:
                    if data.shape[0] > 0:
                        temp_file = join(temp_directory, f'failure_{counter}.csv')
                        data.to_csv(temp_file)
                        data = pd.DataFrame(columns=expected_columns)
                        counter += 1
                bar()

        # merge all CSV files into a single dataframe
        # delete all temp files
        temp_files = get_files_by_type(temp_directory, file_type='csv')
        data = pd.concat(map(read_csv, temp_files))
        data.sort_values(by=['ecode'], ignore_index=True, inplace=True)
        data = data[expected_columns]
    delete_directory(temp_directory)

    return data


def create_csv_dump(target_date, end_time='15:01:00', bar_size='1 min', verbose=False):
    """
        Creates a CSV file from JSON files for a given date.
        Raise an error if directory for the gven is not present.
        CSV files to be saved at the historical data storage location:
            'success.csv' & 'failure.csv'
    """
    logger.info('Generating final CSV dump')
    storage_dir = _setup_storage_directories(target_date, bar_size=bar_size)
    _date = f'{target_date[:4]}/{target_date[4:6]}/{target_date[6:]}'
    write_to_console(f'{"-"*30} CSV Conversion: {_date} {"-"*31}', verbose=True)
    target_directory = join(CACHE_DIR, bar_size.replace(' ', ''), target_date, end_time.replace(':', '_'))

    if not isdir(target_directory):
        raise NotADirectoryError(f'Could not find a data storage directory for date: {target_directory}')

    success_directory = join(target_directory, 'success')
    failure_directory = join(target_directory, 'failure')

    if isdir(success_directory):
        path = join(storage_dir, 'success.csv')
        success = generate_success_dataframe(success_directory, bar_title='Success', verbose=verbose)
        success.to_csv(path, index=False)
        logger.debug(f'Success file saved at: {path}')

    if isdir(failure_directory):
        path = join(storage_dir, 'failure.csv')
        failure = generate_failure_dataframe(failure_directory, bar_title='Failure', verbose=verbose)
        failure.to_csv(path, index=False)
        logger.debug(f'Failure file saved at: {path}')


# noinspection PyUnusedLocal
# TODO: to be deprecated
def generate_extraction_metrics_(target_date, end_time='15:01:00', input_tickers=None, verbose=False):
    """
        Generates metrics about success & failure tickers.
        Metrics are saved into a new file called 'metrics.csv'
        :param target_date: date for which metrics are needed
        :param end_time: end time for metrics are to be generated
        :param input_tickers: tickers for which metrics are to be generated
        :param verbose: display a more detailed output
    """
    logger.info('Generating final extraction metrics')
    _date = f'{target_date[:4]}/{target_date[4:6]}/{target_date[6:]}'
    write_to_console(f'{"-"*30} Metrics Generation: {_date} {"-"*31}', verbose=True)
    expected_metrics = [
        'total_tickers', 'total_extracted', 'total_extraction_ratio',
        'extraction_successful', 'extraction_failure',
        'success_ratio', 'failure_ratio',
        'n_225_input_ratio', 'n_225_success_ratio', 'n_225_failure_ratio',
        'topix_input_ratio', 'topix_success_ratio', 'topix_failure_ratio',
        'jasdaq_20_input_ratio', 'jasdaq_20_success_ratio', 'jasdaq_20_failure_ratio',
        'missing_tickers_ratio', 'missing_tickers'
    ]
    metrics = dict(zip(expected_metrics, [0.0]*len(expected_metrics)))
    target_directory = join(HISTORICAL_DATA_STORAGE, target_date, end_time.replace(':', '_'))
    if not isdir(target_directory):
        raise NotADirectoryError(f'Data storage directory for {target_date} not found at'
                                 f'{HISTORICAL_DATA_STORAGE}')

    success_file = join(target_directory, 'success.csv')
    failure_file = join(target_directory, 'failure.csv')

    if not isfile(success_file):
        raise FileNotFoundError(f'Can not find success file: {success_file}')

    if not isfile(failure_file):
        raise FileNotFoundError(f'Can not find failure file: {failure_file}')

    input_tickers_file = join(target_directory, 'input_tickers.json')
    if input_tickers is None:
        if not isfile(input_tickers_file):
            raise FileNotFoundError(f'Can not find input tickers file: {input_tickers_file}')
        input_tickers = read_json_file(input_tickers_file)

    japan_indices = get_japan_indices()

    _n_225_tickers = japan_indices[japan_indices.n_225.str.contains('T')].n_225.unique().tolist()
    n_225_tickers = list(map(lambda x: int(x.split('.')[0]), _n_225_tickers))

    _topix_tickers = japan_indices[japan_indices.topix.str.contains('T')].topix.unique().tolist()
    topix_tickers = list(map(lambda x: int(x.split('.')[0]), _topix_tickers))

    _jasdaq_20_tickers = japan_indices[japan_indices.jasdaq_20.str.contains('T')].jasdaq_20.unique().tolist()
    jasdaq_20_tickers = list(map(lambda x: int(x.split('.')[0]), _jasdaq_20_tickers))

    success = read_csv(success_file)
    failure = read_csv(failure_file)

    success_tickers = success.ecode.unique().tolist()
    failure_tickers = failure.ecode.unique().tolist()

    total_tickers = len(input_tickers)
    if total_tickers == 0:
        raise ValueError(f'Can not find any input tickers in file {input_tickers_file}')

    extraction_successful = len(success_tickers)
    extraction_failure = len(failure_tickers)
    total_extracted = extraction_successful + extraction_failure
    total_extraction_ratio = round(total_extracted / total_tickers, 3)

    success_ratio = round(extraction_successful / total_tickers, 3)
    failure_ratio = round(extraction_failure / total_tickers, 3)
    logger.debug(f'Updated over-all extraction ratio: {success_ratio}')
    write_to_console(f'Over-all Extraction: {_get_marker(success_ratio)}', pointer='->',
                     indent=2, verbose=True)
    write_to_console(f'Over-all Success Ratio: {success_ratio}',
                     pointer='-', indent=4, verbose=verbose)

    n_225_input = list(set(input_tickers).intersection(n_225_tickers))
    if bool(n_225_input):
        n_225_input_ratio = round(len(n_225_input) / len(n_225_tickers), 3)
        n_225_success = list(set(success_tickers).intersection(n_225_input))
        n_225_failure = list(set(failure_tickers).intersection(n_225_input))
        n_225_success_ratio = round(len(n_225_success) / len(n_225_input), 3)
        n_225_failure_ratio = round(len(n_225_failure) / len(n_225_input), 3)
        logger.debug(f'Updated N225 extraction ratio: {n_225_success_ratio}')
        write_to_console(f'N225 Extraction: {_get_marker(n_225_success_ratio)}', pointer='->',
                         indent=2, verbose=True)
        write_to_console(f'Over-all Success Ratio: {n_225_success_ratio}',
                         pointer='-', indent=4, verbose=verbose)
    else:
        logger.debug('Could not find any N 225 tickers in the given input')

    topix_input = list(set(input_tickers).intersection(topix_tickers))
    if bool(topix_input):
        topix_input_ratio = round(len(topix_input) / len(topix_tickers), 3)
        topix_success = list(set(success_tickers).intersection(topix_input))
        topix_failure = list(set(failure_tickers).intersection(topix_input))
        topix_success_ratio = round(len(topix_success) / len(topix_input), 3)
        topix_failure_ratio = round(len(topix_failure) / len(topix_input), 3)
        logger.debug(f'Updated Topix extraction ratio: {topix_success_ratio}')
        write_to_console(f'Topix Extraction: {_get_marker(topix_success_ratio)}', pointer='->',
                         indent=2, verbose=True)
        write_to_console(f'Topix Success Ratio: {topix_success_ratio}',
                         pointer='-', indent=4, verbose=verbose)
    else:
        logger.debug('Could not find any Topix tickers in the given input')

    jasdaq_20_input = list(set(input_tickers).intersection(jasdaq_20_tickers))
    if bool(jasdaq_20_input):
        jasdaq_20_input_ratio = round(len(jasdaq_20_input) / len(jasdaq_20_tickers), 3)
        jasdaq_20_success = list(set(success_tickers).intersection(jasdaq_20_input))
        jasdaq_20_failure = list(set(failure_tickers).intersection(jasdaq_20_input))
        jasdaq_20_success_ratio = round(len(jasdaq_20_success) / len(jasdaq_20_input), 3)
        jasdaq_20_failure_ratio = round(len(jasdaq_20_failure) / len(jasdaq_20_input), 3)
        logger.debug(f'Updated JASDAQ 20 extraction ratio: {jasdaq_20_success_ratio}')
        write_to_console(f'JASDAQ 20 Extraction: {_get_marker(jasdaq_20_success_ratio)}', pointer='->',
                         indent=2, verbose=True)
        write_to_console(f'JASDAQ 20 Success Ratio: {jasdaq_20_success_ratio}',
                         pointer='-', indent=4, verbose=verbose)
    else:
        logger.debug('Could not find any JASDAQ 20 tickers in the given input')

    missing_tickers = list(set(input_tickers).difference(success_tickers + failure_tickers))
    missing_tickers_ratio = round(len(missing_tickers) / total_tickers, 3)
    logger.debug(f'Updated missing tickers ratio: {missing_tickers_ratio}')

    all_vars = vars()
    for key in all_vars:
        if key in expected_metrics:
            metrics[key] = all_vars[key]

    metrics_file = join(target_directory, 'metrics.json')
    save_data_as_json(metrics, metrics_file)
    logger.debug(f'Metrics saved at: {metrics_file}')


def compute_extraction_metrics(success_data, failure_data, input_data):
    # create aliases for variable names, done only to shorten the code
    s, f, i = success_data, failure_data, input_data

    # over-all metrics
    total = i.code.shape[0]
    extracted = s.ecode.unique().shape[0]
    failed = f.ecode.unique().shape[0]
    missed = total - (extracted + failed)
    extraction_ratio = round(extracted / total, 3) if total > 0 else 0

    # metrics by index
    # topix
    topix = i[i.topix == 1]
    total_topix = topix.code.shape[0]
    extracted_topix = s[s.ecode.isin(topix.code)].ecode.unique().shape[0]
    failed_topix = f[f.ecode.isin(topix.code)].ecode.unique().shape[0]
    missed_topix = total_topix - (extracted_topix + failed_topix)
    extraction_ratio_topix = round(extracted_topix / total_topix, 3) if total_topix > 0 else 0

    # nikkei 225
    nikkei_225 = i[i.nikkei225 == 1]
    total_nikkei225 = nikkei_225.code.shape[0]
    extracted_nikkei225 = s[s.ecode.isin(nikkei_225.code)].ecode.unique().shape[0]
    failed_nikkei225 = f[f.ecode.isin(nikkei_225.code)].ecode.unique().shape[0]
    missed_nikkei225 = total_nikkei225 - (extracted_nikkei225 + failed_nikkei225)
    extraction_ratio_nikkei225 = round(extracted_nikkei225 / total_nikkei225, 3) if total_nikkei225 > 0 else 0

    # jasdaq 20
    jsadaq_20 = i[i.jasdaq20 == 1]
    total_jasdaq20 = jsadaq_20.code.shape[0]
    extracted_jasdaq20 = s[s.ecode.isin(jsadaq_20.code)].ecode.unique().shape[0]
    failed_jasdaq20 = f[f.ecode.isin(jsadaq_20.code)].ecode.unique().shape[0]
    missed_jasdaq20 = total_jasdaq20 - (extracted_jasdaq20 + failed_jasdaq20)
    extraction_ratio_jasdaq20 = round(extracted_jasdaq20 / total_jasdaq20, 3) if total_jasdaq20 > 0 else 0

    # metrics by section
    # first section
    first_section = i[i.section == 'First Section']
    total_first_section = first_section.code.shape[0]
    extracted_first_section = s[s.ecode.isin(first_section.code)].ecode.unique().shape[0]
    failed_first_section = f[f.ecode.isin(first_section.code)].ecode.unique().shape[0]
    missed_first_section = total_first_section - (extracted_first_section + failed_first_section)
    extraction_ratio_first_section = round(extracted_first_section / total_first_section,
                                           3) if total_first_section > 0 else 0

    # second section
    second_section = i[i.section == 'Second Section']
    total_second_section = second_section.code.shape[0]
    extracted_second_section = s[s.ecode.isin(second_section.code)].ecode.unique().shape[0]
    failed_second_section = f[f.ecode.isin(second_section.code)].ecode.unique().shape[0]
    missed_second_section = total_second_section - (extracted_second_section + failed_second_section)
    extraction_ratio_second_section = round(extracted_second_section / total_second_section,
                                            3) if total_second_section > 0 else 0

    # mothers
    mothers = i[i.section == 'Mothers']
    total_mothers = mothers.code.shape[0]
    extracted_mothers = s[s.ecode.isin(mothers.code)].ecode.unique().shape[0]
    failed_mothers = f[f.ecode.isin(mothers.code)].ecode.unique().shape[0]
    missed_mothers = total_mothers - (extracted_mothers + failed_mothers)
    extraction_ratio_mothers = round(extracted_mothers / total_mothers, 3) if total_mothers > 0 else 0

    # jasdaq growth
    jasdaq_growth = i[i.section == 'JASDAQ Growth']
    total_jasdaq_growth = jasdaq_growth.code.shape[0]
    extracted_jasdaq_growth = s[s.ecode.isin(jasdaq_growth.code)].ecode.unique().shape[0]
    failed_jasdaq_growth = f[f.ecode.isin(jasdaq_growth.code)].ecode.unique().shape[0]
    missed_jasdaq_growth = total_jasdaq_growth - (extracted_jasdaq_growth + failed_jasdaq_growth)
    extraction_ratio_jasdaq_growth = round(extracted_jasdaq_growth / total_jasdaq_growth,
                                           3) if total_jasdaq_growth > 0 else 0

    # jasdaq standard
    jasdaq_standard = i[i.section == 'JASDAQ Standard']
    total_jasdaq_standard = jasdaq_standard.code.shape[0]
    extracted_jasdaq_standard = s[s.ecode.isin(jasdaq_standard.code)].ecode.unique().shape[0]
    failed_jasdaq_standard = f[f.ecode.isin(jasdaq_standard.code)].ecode.unique().shape[0]
    missed_jasdaq_standard = total_jasdaq_standard - (extracted_jasdaq_standard + failed_jasdaq_standard)
    extraction_ratio_jasdaq_standard = round(extracted_jasdaq_standard / total_jasdaq_standard,
                                             3) if total_jasdaq_standard > 0 else 0

    # market cap > ¥ 10 B
    mcap_above_10b = i[i.market_cap >= 10e+9]
    total_mcap_above_10b = mcap_above_10b.code.shape[0]
    extracted_mcap_above_10b = s[s.ecode.isin(mcap_above_10b.code)].ecode.unique().shape[0]
    failed_mcap_above_10b = f[f.ecode.isin(mcap_above_10b.code)].ecode.unique().shape[0]
    missed_mcap_above_10b = total_mcap_above_10b - (extracted_mcap_above_10b + failed_mcap_above_10b)
    extraction_ratio_mcap_above_10b = round(extracted_mcap_above_10b / total_mcap_above_10b,
                                            3) if total_mcap_above_10b > 0 else 0

    # 3 month's average trading volume * price >= ¥ 85 MM
    pv_above_85m = i[i.average_trading_volume_3M >= 85e+6]
    total_pv_above_85m = pv_above_85m.code.shape[0]
    extracted_pv_above_85m = s[s.ecode.isin(pv_above_85m.code)].ecode.unique().shape[0]
    failed_pv_above_85m = f[f.ecode.isin(pv_above_85m.code)].ecode.unique().shape[0]
    missed_pv_above_85m = total_mcap_above_10b - (extracted_pv_above_85m + failed_pv_above_85m)
    extraction_ratio_pv_above_85m = round(extracted_pv_above_85m / total_pv_above_85m,
                                          3) if total_pv_above_85m > 0 else 0

    metrics = dict(
        total=total,
        extracted=extracted,
        failed=failed,
        missed=missed,
        extracion_ratio=extraction_ratio,
        total_topix=total_topix,
        extracted_topix=extracted_topix,
        failed_topix=failed_topix,
        missed_topix=missed_topix,
        extraction_ratio_topix=extraction_ratio_topix,
        total_nikkei225=total_nikkei225,
        extracted_nikkei225=extracted_nikkei225,
        failed_nikkei225=failed_nikkei225,
        missed_nikkei225=missed_nikkei225,
        extraction_ratio_nikkei225=extraction_ratio_nikkei225,
        total_jasdaq20=total_jasdaq20,
        extracted_jasdaq20=extracted_jasdaq20,
        failed_jasdaq20=failed_jasdaq20,
        missed_jasdaq20=missed_jasdaq20,
        extraction_ratio_jasdaq20=extraction_ratio_jasdaq20,
        total_first_section=total_first_section,
        extracted_first_section=extracted_first_section,
        failed_first_section=failed_first_section,
        missed_first_section=missed_first_section,
        extraction_ratio_first_section=extraction_ratio_first_section,
        total_second_section=total_second_section,
        extracted_second_section=extracted_second_section,
        failed_second_section=failed_second_section,
        missed_second_section=missed_second_section,
        extraction_ratio_second_section=extraction_ratio_second_section,
        total_mothers=total_mothers,
        extracted_mothers=extracted_mothers,
        failed_mothers=failed_mothers,
        missed_mothers=missed_mothers,
        extraction_ratio_mothers=extraction_ratio_mothers,
        total_jasdaq_growth=total_jasdaq_growth,
        extracted_jasdaq_growth=extracted_jasdaq_growth,
        failed_jasdaq_growth=failed_jasdaq_growth,
        missed_jasdaq_growth=missed_jasdaq_growth,
        extraction_ratio_jasdaq_growth=extraction_ratio_jasdaq_growth,
        total_jasdaq_standard=total_jasdaq_standard,
        extracted_jasdaq_standard=extracted_jasdaq_standard,
        failed_jasdaq_standard=failed_jasdaq_standard,
        missed_jasdaq_standard=missed_jasdaq_standard,
        extraction_ratio_jasdaq_standard=extraction_ratio_jasdaq_standard,
        total_mcap_above_10b=total_mcap_above_10b,
        extracted_mcap_above_10b=extracted_mcap_above_10b,
        failed_mcap_above_10b=failed_mcap_above_10b,
        missed_mcap_above_10b=missed_mcap_above_10b,
        extraction_ratio_mcap_above_10b=extraction_ratio_mcap_above_10b,
        total_pv_above_85m=total_pv_above_85m,
        extracted_pv_above_85m=extracted_pv_above_85m,
        failed_pv_above_85m=failed_pv_above_85m,
        missed_pv_above_85m=missed_pv_above_85m,
        extraction_ratio_pv_above_85m=extraction_ratio_pv_above_85m
    )
    return metrics


def update_metrics_sheet(date, data):
    # create a dataframe for new metrics
    new_metrics = pd.DataFrame(data, index=[0])
    new_metrics['date'] = date
    final_metrics = new_metrics

    # check for existing metrics
    if isfile(DAILY_METRICS_FILE):
        existing_metrics = pd.read_csv(DAILY_METRICS_FILE)
        if date not in existing_metrics.date.values:
            final_metrics = existing_metrics.append(new_metrics, ignore_index=True)
        else:
            final_metrics = existing_metrics

    # adjust order of column headers, 'date' should come first
    columns = ['date'] + list(data.keys())
    metrics = final_metrics.round(decimals=3)[columns]

    # save metrics
    metrics.to_csv(DAILY_METRICS_FILE, index=False)


def generate_daily_extraction_status_sheet(data, input_, location, date):
    extracted_tickers = data.ecode.unique()

    def status_provider(row):
        return 'N/A' if row.status == 'D' else (
            True if row.code in extracted_tickers else False)

    input_['extraction_status'] = input_.apply(status_provider, axis=1)
    status_file = join(location, f'status_{date}.csv')
    input_.to_csv(status_file, index=False)


def metrics_generator(date, bar_size, tickers):
    """
        Generate extraction metrics for daily downloaded data
        Writes data to two new files:
        - metrics.csv: day-wise metrics (success, failed, missed v/s total stocks)
        - status.csv: extraction status for each input ticker for a specific day

        - Parameters:
        -------------
        - data_location(str): location where downloaded data is kept
        - input_file(str): full path to input file
    """
    logger.info('Generating final extraction metrics')
    display_date = f'{date[:4]}/{date[4:6]}/{date[6:]}'
    write_to_console(f'{"-"*30} Metrics Generation: {display_date} {"-"*31}', verbose=True)
    try:
        data_location = join(HISTORICAL_DATA_STORAGE, bar_size.replace(' ', ''),
                             date[:4], MONTH_MAP[int(date[4:6])], date)
        # read success, failure & input files
        success = pd.read_csv(join(data_location, 'success.csv'))
        failure = pd.read_csv(join(data_location, 'failure.csv'))

        if type(tickers) is list:
            pass  # TODO: simple metrics generation
        else:  # assuming that input is a file path
            input_ = pd.read_csv(tickers)
            # filter out relevant input --> active tickers
            relevant_input = input_[input_.status == 'A']

            # get extraction metrics
            metrics = compute_extraction_metrics(success, failure, relevant_input)
            write_to_console(f'Over-all Extraction: {_get_marker(metrics["extracion_ratio"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'Topix Extraction: {_get_marker(metrics["extraction_ratio_topix"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'Nikkei 225 Extraction: {_get_marker(metrics["extraction_ratio_nikkei225"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'JASDAQ 20 Extraction: {_get_marker(metrics["extraction_ratio_jasdaq20"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'First Section Extraction: {_get_marker(metrics["extraction_ratio_first_section"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'Second Section Extraction: '
                             f'{_get_marker(metrics["extraction_ratio_second_section"])}', pointer='->',
                             indent=2, verbose=True)
            write_to_console(f'Mothers Extraction: {_get_marker(metrics["extraction_ratio_mothers"])}',
                             pointer='->', indent=2, verbose=True)
            write_to_console(f'JASDAQ Growth Extraction: '
                             f'{_get_marker(metrics["extraction_ratio_jasdaq_growth"])}', pointer='->',
                             indent=2, verbose=True)
            write_to_console(f'JASDAQ Standard Extraction: '
                             f'{_get_marker(metrics["extraction_ratio_jasdaq_standard"])}', pointer='->',
                             indent=2, verbose=True)
            write_to_console(f'Market Capital Above ¥10B Extraction: '
                             f'{_get_marker(metrics["extraction_ratio_mcap_above_10b"])}', pointer='->',
                             indent=2, verbose=True)
            write_to_console(f'Price x 3 Month\'s Trading Volume ¥85MM Extraction: '
                             f'{_get_marker(metrics["extraction_ratio_pv_above_85m"])}', pointer='->',
                             indent=2, verbose=True)
            # generate / update metrics sheet
            _date = f'{date[:4]}-{date[4:6]}-{date[6:]}'
            update_metrics_sheet(_date, metrics)

            # generate daily extraction status sheet
            generate_daily_extraction_status_sheet(success, input_, data_location, date)
    except Exception as e:
        logger.critical(f'Metrics generation failed: {e}')


if __name__ == '__main__':
    # create_csv_dump('20210121')
    # generate_extraction_metrics('20210120')
    data_location = r'/Users/mandeepsingh/dev/k2q/projects/TWS-Equities/historical_data/20210607/15_01_00'
    input_file = r'/Users/mandeepsingh/dev/k2q/data/input_files/tickers.csv'
    metrics_generator(data_location, input_file)
