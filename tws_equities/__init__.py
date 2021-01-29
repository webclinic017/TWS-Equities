# -*- coding: utf-8 -*-

"""
    ©2021 K2Q Capital Limited.
    All rights reserved.

    This Python package provides access to Historical Bar-Data for Japanese Equities using TWS API.
    Data is pre-formatted into a JSON object for immediate consumption, an error stack will be provided in
    case of a failure.

    This package also provides a CLI to easily the built-in functionalities, run the follwoing command in
    your terminal to view all the options & commands available:
        - python -m tws_equities -h
"""


from tws_equities.parsers import parse_user_args
from tws_equities.controller import setup_logger
from tws_equities.controller import run
from tws_equities.controller import download
from tws_equities.controller import convert
from tws_equities.controller import metrics


RED_CROSS = u'\u274C'
GREEN_TICK = u'\u2705'

__author__ = {'Mandeep Singh'}
__copyright__ = '© 2021 K2Q Capital Limited'
__license__ = 'MIT'

# version
__major__ = 1
__minor__ = 0
__micro__ = 0
__version__ = f'{__major__}.{__minor__}.{__micro__}'


COMMAND_MAP = {
                    'run': run,
                    'download': download,
                    'convert': convert,
                    'metrics': metrics
              }

__all__ = [
                'parse_user_args',
                'setup_logger',
                'run',
                'download',
                'convert',
                'metrics',
                'COMMAND_MAP',
                'RED_CROSS',
                'GREEN_TICK'
          ]
