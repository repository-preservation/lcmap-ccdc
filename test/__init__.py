import os
import sys

CWD = os.path.dirname(os.path.realpath(__file__))

def data_config():
    """ Controls the test data that is loaded into the system """
    return {'x': -1821585,
            'y': 2891595,
            'acquired': '1982-01-01/2015-12-12',
            'dataset_name': 'ARD',
            'specs_url': os.getenv('SPECS_URL'),
            'chips_url': os.getenv('CHIPS_URL'),
            'chips_dir': os.path.join(CWD, 'resources/data/chips'),
            'specs_dir': os.path.join(CWD, 'resources/data/specs')}
