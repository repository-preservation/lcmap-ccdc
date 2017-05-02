"""
Aardvark interface module for firebird.  aardvark.py contains non-composed
functions for working with aardvark.  It is intended to enable a number of
use cases whether the calling code requires stacks or time-series, wide
spatial extent data, or any mix of different data spectra.

Features include:
    - query aardvark for chips
    - query aardvark for chip_specs
    - convert encoded chip data to numpy arrays
    - split chip data into data structures that properly identify each data by
      x,y,t,s,u where x is longitude, y is latitude, t is time s is spectra
      and u is ubid
    - merge split data into a 'rainbow' data structure for input to pyccd.

It is up the caller of the module to compose these functions together properly.
There is a natural order that they can be composed however, as the expected
input parameters (and types) have been carefully examined to ensure they are
compatible with upstream return values.

Do not add functions into this module that implement business logic, such as
'how' the functions are used together.  That should be done in the
calling modules.  aardvark.py is a client library only.
"""
import requests
from firebird import chip


def pyccd_chip_spec_queries(url):
    """
    TODO: This should really be housed elsewhere.  It is specific to pyccd.
    A map of pyccd spectra to chip-spec queries
    :param url: full url (http://host:port/context) for chip-spec endpoint
    :return: map of spectra to chip spec queries
    :example:
    >>> pyccd_chip_spec_queries('http://host/v1/landsat/chip-specs')
    {"red":     'http://host/v1/landsat/chip-specs?q=tags:red AND sr',
     "green":   'http://host/v1/landsat/chip-specs?q=tags:green AND sr'
     "blue":    'http://host/v1/landsat/chip-specs?q=tags:blue AND sr'
     "nir":     'http://host/v1/landsat/chip-specs?q=tags:nir AND sr'
     "swir1":   'http://host/v1/landsat/chip-specs?q=tags:swir1 AND sr'
     "swir2":   'http://host/v1/landsat/chip-specs?q=tags:swir2 AND sr'
     "thermal": 'http://host/v1/landsat/chip-specs?q=tags:thermal AND ta'
     "qa":      'http://host/v1/landsat/chip-specs?q=tags:qa AND tags:pixel'}
    """
    return {"red":     ''.join([url, '?q=tags:red AND sr']),
            "green":   ''.join([url, '?q=tags:green AND sr']),
            "blue":    ''.join([url, '?q=tags:blue AND sr']),
            "nir":     ''.join([url, '?q=tags:nir AND sr']),
            "swir1":   ''.join([url, '?q=tags:swir1 AND sr']),
            "swir2":   ''.join([url, '?q=tags:swir2 AND sr']),
            "thermal": ''.join([url, '?q=tags:bt AND thermal AND NOT tirs2']),
            "qa":      ''.join([url, '?q=tags:pixelqa'])}


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


def sort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(sorted(chips, key=lambda c: c['acquired'], reverse=True))


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


def split(chip_idx, chip_idy, chip_specs_byubid, chips):
    """
    Accepts a sequence of chips plus location information and returns
    sequences of pixels organized by x,y,t for all chips.
    :param chip_idx:  x coordinate for the chip id
    :param chip_idy:  y coordinate for the chip id
    :param chip_spec: chip spec for the chip array
    :param chips: sequence of chips
    :type chip_idx: number
    :type chip_idy: number
    :type chip_specs_byubid: dictionary of chip_specs keyed by ubid
    :type chips: sequence of chips with data as numpy arrays
    :returns: sequence of (x, y, date, ubid, data) for each x,y
    {(x, y): {t: data}}
    """
    pass


def merge(split_chips):
    """
    Combines multiple spectral sequences into a single multi-dimensional
    sequence, ready for pyccd execution.
    """
    pass
