import glob
import json
import os
import itertools
import test
from firebird.files import read

CHIPS_DIR = test.test_data_config()['chips_dir']
SPECS_DIR = test.test_data_config()['specs_dir']

def flatten(iterable):
    """
    Reduce dimensionality of iterable containing iterables
    :param iterable: A multi-dimensional iterable
    :returns: A one dimensional iterable
    """
    return itertools.chain.from_iterable(iterable)


def chips(spectra, root_dir=CHIPS_DIR):
    """
    Return chips for named spectra
    :param spectra: red, green, blue, nir, swir1, swir2, thermal or cfmask
    :type spectra: string
    :returns: sequence of chips
    """
    path = ''.join([root_dir, os.sep, '*', spectra, '*'])
    filenames = glob.glob(path)
    chips = [json.loads(read(filename)) for filename in filenames]
    return flatten(chips)


def chip_specs(spectra, root_dir=SPECS_DIR):
    """
    Returns chip specs for the named spectra.
    :param spectra: red, green, blue, nir, swir1, swir2, thermal or cfmask
    :type spectra: string
    :returns: sequence of chip specs
    """
    path = ''.join([root_dir, os.sep, '*', spectra, '*'])
    filenames = glob.glob(path)
    return json.loads(read(filenames[0]))


def chip_ids(root_dir=CHIPS_DIR):
    """
    Returns chip ids for available chip data in root_dir
    :param root_dir: directory where band data resides
    :return: tuple of tuples of chip ids (UL coordinates)
    """
    def getxy(fpath):
        _fs = fpath.split('_')
        return _fs[1], _fs[2]

    return tuple({getxy(i) for i in glob.glob(''.join([root_dir, os.sep, '*blue*']))})
