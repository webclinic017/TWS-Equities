#! TWS-Project/venv/bin/python3.9


"""
    Common configurations
"""


from os.path import abspath
from os.path import join
from pathlib import Path


BASE_DIR = Path(abspath(__file__)).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_equities')
CACHE_DIR = join(BASE_DIR, '.cache')
HISTORICAL_DATA_STORAGE = join(BASE_DIR, 'historical_data')
DAILY_METRICS_FILE = join(HISTORICAL_DATA_STORAGE, 'metrics.csv')
