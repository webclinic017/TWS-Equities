# -*- coding: utf-8 -*-

from logging import addLevelName
from logging.config import dictConfig
from logging import getLogger

from tws_equities.helpers import dt
from tws_equities.helpers import join
from tws_equities.helpers import isdir
from tws_equities.helpers import makedirs
from tws_equities.helpers import get_project_root

# from tws_equities.helpers import PROJECT_ROOT


# from datetime import datetime as dt
# from os.path import dirname
# from os.path import join
# from os.path import isdir
# from os import makedirs


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'file': {
            'format': '%(asctime)s | %(name)s:%(levelname)s | '
                      '%(module)s:%(funcName)s:%(lineno)d | %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'console': {
            'format': '%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'level': 'CRITICAL',
            'formatter': 'console',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        'file': {
            'level': 'ERROR',
            'formatter': 'file',
            'class': 'logging.FileHandler',
            'encoding': 'utf-8',
            'filename': 'app.log'
        }
    },
    'loggers': {
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        },
        'child': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}
LOG_LEVEL_MAP = {
    10: 'DEBUG',
    20: 'INFO',
    30: 'WARNING',
    40: 'ERROR',
    50: 'CRITICAL'
}
LOG_COLOR_MAP = {
    'DEBUG': '\x1b[32;1m',  # green
    'INFO': '\x1b[34;1m',  # blue
    'WARNING': '\x1b[33;1m',  # yellow
    'ERROR': '\x1b[31;1m',  # red
    'CRITICAL': '\x1b[31;7m'  # bg: red | fg: black
}
LOG_LOCATION = join(get_project_root(), 'logs')


def get_log_file():
    """
        Sets up log directory , creates a log file for current run and return full path for the same.
    """
    date_format, time_format = '%Y-%m-%d', '%H_%M_%S'
    current_date_time = dt.today()
    date = dt.strftime(current_date_time, date_format)
    time = dt.strftime(current_date_time, time_format)
    log_location = join(LOG_LOCATION, date)
    if not(isdir(log_location)):
        makedirs(log_location)
    log_file_name = join(log_location, f'{time}.log')
    return log_file_name


def update_logging_config(name, level):
    """
        Update log level and add a log file in the global configuration.
    """
    # update logging level based on user input
    for handler in LOGGING_CONFIG['handlers']:
        LOGGING_CONFIG['handlers'][handler]['level'] = level

    # user wants to debug a problem, start writing logs to a file
    if level == 'DEBUG':
        LOGGING_CONFIG['handlers']['file']['filename'] = get_log_file()
        LOGGING_CONFIG['loggers'][name]['handlers'].append('file')


def get_logger(name, verbose=False, debug=False, colored=False):
    """
        Initialize & return logger
        :param name: name of the logger
        :param verbose: returns a console logger, with log level set to INFO
        :param debug: returns a file logger, with log level set to DEBUG
        :param colored: color highlight log level based on severity(recommended for logging to console), default=False
        :return: logger object
    """
    name = 'root' if name == '__main__' else 'child'
    level = 'DEBUG' if debug else 'INFO' if verbose else 'WARNING'
    if name == 'root':  # do this only once, for the root logger
        update_logging_config(name, level)

    # load logging config
    dictConfig(LOGGING_CONFIG)

    # init logger
    logger = getLogger(name)

    # add color support for log level
    if colored:
        for level, label in LOG_LEVEL_MAP.items():
            color = LOG_COLOR_MAP.get(label, '\x1b[0m')
            addLevelName(level, f'{color}{label}\x1b[0m')
    return logger


def clean_obsolete_logs(duration=7):
    """
        Cleans logs files and directories older than given duration.
        :param duration: number of days prior to which logs are to be cleaned
    """
    if not isinstance(duration, int):
        raise TypeError('Duration must be an integer.')
    if duration < 1:
        raise ValueError('Duration must be a positive integer.')

    pass


if __name__ == '__main__':
    logger = get_logger(__name__, verbose=True, debug=True, colored=True)
    logger.debug('test debug')
    logger.info('test info')
    logger.warning('test warning')
    logger.error('test error')
    logger.critical('test critical')
