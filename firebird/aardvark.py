import requests
from . import chip


def pyccd_chip_spec_queries(url):
    """
    A map of pyccd spectra to chip-spec queries
    :param url: full url for chip-spec endpoint
    :return: map of spectra to chip spec queries
    :example:
    >>> pyccd_chip_spec_queries('http://host:port/landsat/chip-specs')
    {"red":     'http://host:port/landsat/chip-specs?q=tags:red AND sr',
     "green":   'http://host:port/landsat/chip-specs?q=tags:green AND sr'
     "blue":    'http://host:port/landsat/chip-specs?q=tags:blue AND sr'
     "nir":     'http://host:port/landsat/chip-specs?q=tags:nir AND sr'
     "swir1":   'http://host:port/landsat/chip-specs?q=tags:swir1 AND sr'
     "swir2":   'http://host:port/landsat/chip-specs?q=tags:swir2 AND sr'
     "thermal": 'http://host:port/landsat/chip-specs?q=tags:thermal AND toa'
     "cfmask":  'http://host:port/landsat/chip-specs?q=tags:cfmask AND sr'}
    """
    return {"red":     ''.join([url, '?q=tags:red AND sr']),
            "green":   ''.join([url, '?q=tags:green AND sr']),
            "blue":    ''.join([url, '?q=tags:blue AND sr']),
            "nir":     ''.join([url, '?q=tags:nir AND sr']),
            "swir1":   ''.join([url, '?q=tags:swir1 AND sr']),
            "swir2":   ''.join([url, '?q=tags:swir2 AND sr']),
            "thermal": ''.join([url, '?q=tags:thermal AND toa']),
            "cfmask":  ''.join([url, '?q=tags:cfmask AND sr'])}


def chip_specs(query):
    """
    Queries elasticsearch and returns chip_specs
    :param query: full url query for elasticsearch
    :returns: sequence of chip specs
    :example:
    >>> chip_specs('http://host:port/landsat/chip-specs?q=tags:red AND sr')
    ['chip_spec_1', 'chip_spec_2', ...]
    """
    js = requests.get(query).json()
    if 'hits' in js and 'hits' in js['hits']:
        return [hit['_source'] for hit in js['hits']['hits']]
    else:
        return []


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
    return [cs['ubid'] for cs in chip_specs if 'ubid' in cs]


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
    return requests.get(url, params={'x': x,
                                     'y': y,
                                     'acquired': acquired,
                                     'ubids': ubids}).json()


def sort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return sorted(chips, key=lambda c: c['acquired'])


def dates(chips):
    """
    Dates for a sequence of chips
    :param chips: sequence of chips
    :returns: sequence of dates
    """
    return [c['acquired'] for c in chips]


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


def filter(chips, dates):
    """
    Eliminates chips that are not from the specified dates
    :param chips: Sequence of chips
    :param dates: Sequence of dates
    :returns: Sequence of filtered chips
    """
    return filter(lambda c: c['acquired'] in dates, chips)


def to_numpy(chips, chip_specs):
    """
    Converts the data for a sequence of chips to numpy arrays
    :param chips: a sequence of chips
    :param chip_specs: chip_spec dict keyed by ubid
    :returns: sequence of chips with data as numpy arrays
    """
    return map(lambda c: chip.to_numpy(c, chip_specs[c['ubid']]), chips)


def split(chip_idx, chip_idy, chip_spec, chips):
    """
    Accepts a sequence of chips plus location information and returns
    sequences of pixels organized by x,y,t for all chips.
    :param chip_idx:  x coordinate for the chip id
    :param chip_idy:  y coordinate for the chip id
    :param chip_spec: chip spec for the chip array
    :param chips: sequence of chips
    :type chip_idx: number
    :type chip_idy: number
    :type chip_spec: dictionary
    :type chips: sequence of chips with data as numpy arrays
    :returns: sequence of (x, y, t sequence, pixel data sequence) for each x,y
    """
    pass


def merge():
    """
    Combines multiple spectral sequences into a single multi-dimensional
    sequence, ready for pyccd execution.
    """
    pass
