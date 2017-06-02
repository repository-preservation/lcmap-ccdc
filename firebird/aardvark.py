"""
Aardvark interface module for firebird.  aardvark.py contains non-composed
functions for working with aardvark.  It is intended to enable a number of
use cases whether the calling code requires stacks of time-series, wide
spatial extent data, or any mix of different data spectra.

Features include:
    - query aardvark for chips
    - query aardvark for chip_specs
    - convert encoded chip data to numpy arrays
    - split chip data into x, y, t rods
    - assign location identifiers to each rod

It is up the caller of the module to compose these functions together properly.
There is a natural order that they can be composed however, as the expected
input parameters (and types) have been carefully examined to ensure they are
compatible with upstream return values.

Do not add functions into this module that implement business logic, such as
'how' the functions are used together.  That should be done in the
calling modules.  aardvark.py is a client library only.
"""
import requests
import numpy as np
from firebird import chip


def chip_specs(query):
    """
    Queries aardvark and returns chip_specs
    :param query: full url query for aardvark
    :returns: sequence of chip specs
    :example:
    >>> chip_specs('http://host:port/v1/landsat/chip-specs?q=red AND sr')
    ('chip_spec_1', 'chip_spec_2', ...)
    """
    return tuple(requests.get(query).json())


def byubid(chip_specs):
    """
    Organizes chip_specs by ubid
    :param chip_specs: a sequence of chip specs
    :returns: a dict of chip_specs keyed by ubid
    """
    return {cs['ubid']: cs for cs in chip_specs}


def ubids(chip_specs):
    """
    Extract ubids from a sequence of chip_specs
    :param chip_specs: a sequence of chip_spec dicts
    :returns: a sequence of ubids
    """
    return tuple(cs['ubid'] for cs in chip_specs if 'ubid' in cs)


def chips(url, x, y, acquired, ubids):
    """
    Returns aardvark chips for given x, y, date range and ubid sequence
    :param url: full url to aardvark endpoint
    :param x: longitude
    :param y: latitude
    :param acquired: date range as iso8601 strings '2012-01-01/2014-01-03'
    :param ubids: sequence of ubid strings
    :type url: string
    :type x: number
    :type y: number
    :type acquired: string
    :type ubids: sequence
    :returns: TBD

    :example:
    >>> chips(url='http://host:port/landsat/chips',
              x=123456,
              y=789456,
              acquired='2012-01-01/2014-01-03',
              ubids=['LANDSAT_7/ETM/sr_band1', 'LANDSAT_5/TM/sr_band1'])
    """
    return tuple(requests.get(url, params={'x': x,
                                           'y': y,
                                           'acquired': acquired,
                                           'ubid': ubids}).json())

def dates(chips):
    """
    Dates for a sequence of chips
    :param chips: sequence of chips
    :returns: sequence of dates
    """
    return tuple([c['acquired'] for c in chips])


def intersection(items):
    """
    Returns the intersecting set contained in items
    :param items: Two dimensional sequence of items
    :returns: Intersecting set of items
    :example:
    >>> items = [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    >>> intersection(items)
    {3}
    """
    return set.intersection(*(map(lambda x: set(x), items)))


def trim(chips, dates):
    """
    Eliminates chips that are not from the specified dates
    :param chips: Sequence of chips
    :param dates: Sequence of dates
    :returns: Sequence of filtered chips
    """
    return tuple(filter(lambda c: c['acquired'] in dates, chips))


def to_numpy(chips, chip_specs_byubid):
    """
    Converts the data for a sequence of chips to numpy arrays
    :param chips: a sequence of chips
    :param chip_specs_byubid: chip_spec dict keyed by ubid
    :returns: sequence of chips with data as numpy arrays
    """
    return map(lambda c: chip.to_numpy(c, chip_specs_byubid[c['ubid']]), chips)


def rods(chips):
    """
    Accepts sequences of chips and returns
    time series pixel rods organized by x, y, t for all chips.
    Chips should be sorted as desired before calling rods() as outputs
    preserve input order.
    :param chips: sequence of chips
    :type chips: sequence of chips with data as numpy arrays
    :returns: 3d numpy array organized by x, y, and t.  Output shape matches
              input chip shape with the chip value replaced by
              another numpy array of chip time series values
    :description:
    1. For each chip add data to master numpy array.
    2. Transpose the master array
    3. Horizontally stack the transposed master array elements
    4. Reshape the master array to match incoming chip dimensions
    5. Pixel rods are now organized for timeseries access by x, y, t

    >>> chip_one   = np.int_([[11, 12, 13],
                              [14, 15, 16],
                              [17, 18, 19]])

    >>> chip_two   = np.int_([[21, 22, 23],
                              [24, 25, 26],
                              [27, 28, 29]])

    >>> chip_three  = np.int_([[31, 32, 33],
                               [34, 35, 36],
                               [37, 38, 39]])

    >>> master = np.conj([chip_one, chip_two, chip_three])
    >>> np.hstack(master.T).reshape(3, 3, -1)
    array([[[ 11, 21, 31], [ 12, 22, 32], [ 13, 23, 33]],
           [[ 14, 24, 34], [ 15, 25, 35], [ 16, 26, 36]],
           [[ 17, 27, 37], [ 18, 28, 38], [ 19, 29, 39]]])
    """
    master = np.conj([c['data'] for c in chips])
    return np.hstack(master.T).reshape(*master[0].shape,-1)


def locrods(locations, rods):
    """
    Combines location information with pixel rods.
    :param locations:  Chip shaped numpy array of locations
    :param rods: Chip shaped numpy array of rods
    :returns: dict of (location): rod for each location and rod in the arrays.
    :description:
    Incoming locations as 3d array:

    array([[[0, 0], [0, 1], [0, 2]],
           [[1, 0], [1, 1], [1, 2]],
           [[2, 0], [2, 1], [2, 2]]])

    Incoming rods also as 3d array:

    array([[[110, 110, 234, 664], [ 23, 887, 110, 111], [110, 464, 223, 112]],
          [[111, 887,   1, 110],  [ 33, 111,  12, 111], [  0, 111,  66, 112]],
          [[ 12,  99, 112, 110],  [112,  87, 231, 111], [112,  45,  47, 112]]])

    locrods converts locations to:
    >>> locations.reshape(locations.shape[0] * locations.shape[1], -1)
    array([[0, 0],
           [0, 1],
           [0, 2],
           [1, 0],
           [1, 1],
           [1, 2],
           [2, 0],
           [2, 1],
           [2, 2]])

    And rods to:
    >>> rods.reshape(rods.shape[0] * rods.shape[1], -1)
    array([[110, 110, 234, 664],
           [ 23, 887, 110, 111],
           [110, 464, 223, 112],
           [111, 887,   1, 110],
           [ 33, 111,  12, 111],
           [  0, 111,  66, 112],
           [ 12,  99, 112, 110],
           [112,  87, 231, 111],
           [112,  45,  47, 112]])

    Then the locations and rods are zipped together into a dictionary
    comprehension and returned.
    {
     (0, 0): [110, 110, 234, 664],
     (0, 1): [ 23, 887, 110, 111],
     (0, 2): [110, 464, 223, 112],
     (1, 0): [111, 887,   1, 110],
     (1, 1): [ 33, 111,  12, 111],
     (1, 2): [  0, 111,  66, 112],
     (2, 0): [ 12,  99, 112, 110],
     (2, 1): [112,  87, 231, 111],
     (2, 2): [112,  45,  47, 112]
    }
    """
    flat_locs = locations.reshape(locations.shape[0] * locations.shape[1], -1)
    flat_rods = rods.reshape(rods.shape[0] * rods.shape[1], -1)
    return {tuple(k):v for k,v in zip(flat_locs, flat_rods)}
