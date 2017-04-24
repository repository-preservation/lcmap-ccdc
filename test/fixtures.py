import pytest
import glob
import json
import os
import itertools

def read(path):
    with open(path, 'r+') as handle:
        return handle.read()

def chips(spectra, root_dir='./resources/data/chips/band-json'):
    path = ''.join([root_dir, os.sep, '*', spectra, '*'])
    filenames = glob.glob(path)
    chips = [json.loads(read(filename)) for filename in filenames]
    return itertools.chain.from_iterable(chips)

def chip_specs(spectra, root_dir='./resources/data/chip-specs'):
    """ Returns chip specs for the named spectra.
        Args:
            spectra: red, green, blue, nir, swir1, swir2, thermal or cfmask
        Returns:
            Chip specs
    """
    path = ''.join([root_dir, os.sep, '*', spectra, '*'])
    filenames = glob.glob(path)
    return json.loads(read(filenames[0]))
