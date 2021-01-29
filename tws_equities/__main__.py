#! TWS-Project/venv/bin/python3.9
# -*- coding: utf-8 -*-

from sys import stdout
from sys import stderr
# from logging import getLogger
# from logging import  Formatter
# from logging.handlers import TimedRotatingFileHandler

from tws_equities import parse_user_args
from tws_equities import get_logger
from tws_equities import COMMAND_MAP
from tws_equities import RED_CROSS as _RED_CROSS

# load user input
user_args = parse_user_args()

# extract command and remove the key
# command is only to be used at the top-level, to trigger underlying functionality
command = user_args['command']
del user_args['command']

# setup root logger
debug = user_args['debug']
del user_args['debug']
logger = get_logger(__name__, debug=debug)


def main():
    try:
        logger.info(f'Parsed user arguments, triggering target function for: {command}')
        target_function = COMMAND_MAP[command]
        target_function(**user_args)
    except KeyboardInterrupt:
        _message = 'Detected keyboard interruption from the user, terminating program....'
        stderr.write(f'{_RED_CROSS} {_message}\n')
        logger.error(_message)
    except Exception as e:
        _message = f'Program Crashed: {e}'
        stderr.write(f'{_RED_CROSS} {_message}\n')
        logger.critical(_message, exc_info=True)
        if debug:
            raise e
    # TODO: run final cleanup here
    stderr.flush()
    stdout.flush()


if __name__ == '__main__':
    main()
