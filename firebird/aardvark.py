import json
import requests


def pyccd_tile_spec_queries(url):
    """ returns map of pyccd spectra to tile-spec queries
        params:
            url - full url for tile-spec endpoing
                  http://localhost:9200/landsat/tile-specs
    """
    return {"red"     : ''.join([url, '?q=tags:red AND sr']),
            "green"   : ''.join([url, '?q=tags:green AND sr']),
            "blue"    : ''.join([url, 'q=tags:blue AND sr']),
            "nir"     : ''.join([url, '?q=tags:nir AND sr']),
            "swir1"   : ''.join([url, '?q=tags:swir1 AND sr']),
            "swir2"   : ''.join([url, '?q=tags:swir2 AND sr']),
            "thermal" : ''.join([url, '?q=tags:thermal AND toa']),
            "cfmask"  : ''.join([url, '?q=tags:cfmask AND sr'])}


def tile_specs(query):
    """ queries elasticsearch and returns tile_specs
        params:
            query - full url query for elasticsearch
                    http://localhost:9200/landsat/tile-specs?q=tags:red AND sr
        returns:
            ['tile_spec_1', 'tile_spec_2', ...]
    """
    js = requests.get(query).json()
    if 'hits' in js and 'hits' in js['hits']:
        return [hit['_source'] for hit in js['hits']['hits']]
    else:
        return []


def ubids(tile_spec):
    """ return a sequence of ubids from a tile_spec
        params:
            tile_spec - a tile_spec dict
        returns:
            a sequence of ubids ['ubid1', 'ubid2', 'ubid3' ...]
    """
    return tile_spec['ubid']


def data(url, x, y, acquired, ubids):
    """ returns aardvark data for given x, y, date range and ubid sequence
        params:
            url - full url to aardvark endpoint
            x - number, longitude
            y - number, latitude
            acquired - date range as iso8601 strings '2012-01-01/2014-01-03'
            ubids - sequence of ubid strings
        returns:
            TBD
        example:
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

