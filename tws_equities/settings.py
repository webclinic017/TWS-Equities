#! TWS-Project/venv/bin/python3.9


"""
    Common configurations
"""


from os.path import abspath
from os.path import join
from pathlib import Path


# common variables
DEBUG = False

# project related directories/files
BASE_DIR = Path(abspath(__file__)).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_equities')
CACHE_DIR = join(BASE_DIR, '.cache')
HISTORICAL_DATA_STORAGE = join(BASE_DIR, 'historical_data')
DAILY_METRICS_FILE = join(HISTORICAL_DATA_STORAGE, 'metrics.csv')

# status indicators  --> CROSS = BAD | TICK = GOOD
RED_CROSS = u'\u274C'
GREEN_TICK = u'\u2705'

# month map, used to convert integer values to string for months
MONTH_MAP = {
                1: 'january',
                2: 'february',
                3: 'march',
                4: 'april',
                5: 'may',
                6: 'june',
                7: 'july',
                8: 'august',
                9: 'september',
                10: 'october',
                11: 'november',
                12: 'december'
            }
