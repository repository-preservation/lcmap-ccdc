"""
import logging
import os

LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'DEBUG')

# configure logging
__numeric_level = getattr(logging, LOGGING_LEVEL.upper(), None)
if not isinstance(__numeric_level, int):
    raise ValueError('Invalid log level: %s' % LOGGING_LEVEL)
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=__numeric_level)
"""
