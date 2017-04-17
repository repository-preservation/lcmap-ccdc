import json
import requests


def pyccd_tile_spec_queries(url):
    """
    A map of pyccd spectra to tile-spec queries
    Args:
        url: full url for tile-spec endpoint
             http://localhost:9200/landsat/tile-specs
    """
    return {"red"     : ''.join([url, '?q=tags:red AND sr']),
            "green"   : ''.join([url, '?q=tags:green AND sr']),
            "blue"    : ''.join([url, '?q=tags:blue AND sr']),
            "nir"     : ''.join([url, '?q=tags:nir AND sr']),
            "swir1"   : ''.join([url, '?q=tags:swir1 AND sr']),
            "swir2"   : ''.join([url, '?q=tags:swir2 AND sr']),
            "thermal" : ''.join([url, '?q=tags:thermal AND toa']),
            "cfmask"  : ''.join([url, '?q=tags:cfmask AND sr'])}


def chip_specs(query):
    """
    Queries elasticsearch and returns chip_specs
    Args:
        query: full url query for elasticsearch
               http://localhost:9200/landsat/tile-specs?q=tags:red AND sr
    Returns:
        ['chip_spec_1', 'chip_spec_2', ...]
    """
    js = requests.get(query).json()
    if 'hits' in js and 'hits' in js['hits']:
        return [hit['_source'] for hit in js['hits']['hits']]
    else:
        return []


def ubids(tile_specs):
    """
    Extract ubids from a sequence of tile_specs
    Args:
        tile_specs: a sequence of tile_spec dicts
    Returns:
        a sequence of ubids
    """
    return [ts['ubid'] for ts in tile_specs if 'ubid' in ts]


def data(url, x, y, acquired, ubids):
    """
    Returns aardvark data for given x, y, date range and ubid sequence
    Args:
        url: full url to aardvark endpoint
        x: number, longitude
        y: number, latitude
        acquired: date range as iso8601 strings '2012-01-01/2014-01-03'
        ubids: sequence of ubid strings
    Returns:
        TBD
    Example:
        data(url='http://localhost:5678/landsat/tiles',
             x=123456,
             y=789456,
             acquired='2012-01-01/2014-01-03',
             ubids=['LANDSAT_7/ETM/sr_band1', 'LANDSAT_5/TM/sr_band1'])
    """
    return requests.get(url, params={'x': x,
                                     'y': y,
                                     'acquired': acquired,
                                     'ubids': ubids}).json()

#;(defn pyccd-inputs
#  ;[polygon start_date end_date]
#  ;None)


#;def chip_ids(bbox, tile_spec):
#;    """ Returns all the chip ids (tile_x, tile_y points) needed to
#;        cover the bbox """
#;    pass
