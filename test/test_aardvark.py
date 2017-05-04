from firebird.aardvark import pyccd_chip_spec_queries
from firebird.aardvark import ubids
from firebird.aardvark import byubid
from firebird.aardvark import sort
from firebird.aardvark import dates
from firebird.aardvark import intersection
from firebird.aardvark import trim
from firebird.aardvark import to_numpy
from fixtures import chip_specs

from base64 import b64encode
from functools import reduce
from itertools import product
from hypothesis import given
import hypothesis.strategies as st
import urllib
import numpy as np

@given(url=st.sampled_from(('http://localhost',
                            'https://localhost',
                            'http://localhost/',
                            'http://127.0.0.1')))
def test_pyccd_chip_spec_queries(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netloc
    queries = pyccd_chip_spec_queries(url)
    [check(query) for query in queries.values()]


def test_byubid():
    inputs = list()
    inputs.append({'ubid': 'a', 'data': None})
    inputs.append({'ubid': 'b', 'data': None})
    inputs.append({'ubid': 'c', 'data': None})
    results = byubid(inputs)
    # check that dicts were rekeyed into a new dict
    assert all(map(lambda r: r in results, ['a', 'b', 'c']))
    # check structure of new dict values
    assert all(map(lambda r: 'ubid' in r and 'data' in r, results.values()))


def test_ubids():
    data = ({'ubid': 'a/b/c'}, {'ubid': 'd/e/f'}, {'ubid': 'g'}, {'nope': 'z'})
    good = filter(lambda f: 'ubid' in f, data)
    assert set(map(lambda u: u['ubid'], good)) == set(ubids(data))


def test_ubids_from_chip_specs():
    assert len(ubids(chip_specs('blue'))) == 4


def test_sort():
    inputs = list()
    inputs.append({'acquired': '2015-04-01'})
    inputs.append({'acquired': '2017-04-01'})
    inputs.append({'acquired': '2017-01-01'})
    inputs.append({'acquired': '2016-04-01'})
    results = sort(inputs)
    assert(results[0]['acquired'] > results[1]['acquired'] >
           results[2]['acquired'] > results[3]['acquired'])


def test_dates():
    inputs = list()
    inputs.append({'acquired': '2015-04-01'})
    inputs.append({'acquired': '2017-04-01'})
    inputs.append({'acquired': '2017-01-01'})
    inputs.append({'acquired': '2016-04-01'})
    assert set(dates(inputs)) == set(map(lambda d: d['acquired'], inputs))


def test_intersection():
    items = [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    assert intersection(items) == {3}


def test_trim():
    inputs = list()
    inputs.append({'include': True, 'acquired': '2015-04-01'})
    inputs.append({'include': True, 'acquired': '2017-04-01'})
    inputs.append({'include': False, 'acquired': '2017-01-01'})
    inputs.append({'include': True, 'acquired': '2016-04-01'})
    included = dates(filter(lambda d: d['include'] is True, inputs))
    trimmed = trim(inputs, included)
    assert len(list(trimmed)) == len(included)
    assert set(included) == set(map(lambda x: x['acquired'], trimmed))


def test_to_numpy():
    """ Builds combos of shapes and numpy data types and tests
        aardvark.to_numpy() with all of them """

    # define the types we want to make sure to_numpy can handle
    types = ('uint8', 'uint16', 'int8', 'int16')

    # define the shapes we want to make sure it handles.  This is to ensure
    # that to_numpy doesn't do something bad like altering the data shape.
    shapes = ((3, 3), (1, 1), (100, 100))

    def _ubid(dtype, shape):
        """ generate ubid for test data """
        return '_'.join([dtype, str(shape)])

    def _chips(dtype, shape, ubid):
        """ generate chips for test data """
        limits = np.iinfo(dtype)
        samples = reduce(lambda accum, v: accum * v, shape)
        data = np.random.randint(limits.min,
                                 limits.max,
                                 samples,
                                 dtype).reshape(shape)
        return {'ubid': ubid,
                'data': b64encode(data)}

    def _specs(dtype, shape, ubid):
        """ generate chip specs for test data """
        return {'ubid': ubid,
                'data_shape': shape,
                'data_type': dtype.upper()}

    # build every combination of types and shapes to test against
    combos = tuple(product(types, shapes))

    # generate the chip_specs and chips
    chips = [_chips(*c, _ubid(*c)) for c in combos]
    specs = [_specs(*c, _ubid(*c)) for c in combos]
    specs_byubid = byubid(specs)

    # make assertions about results
    for npchip in to_numpy(chips, specs_byubid):
        ubid = npchip['ubid']
        spec = specs_byubid[ubid]
        assert npchip['data'].dtype.name == spec['data_type'].lower()
        assert npchip['data'].shape == spec['data_shape']


def test_rods():
    pass


def test_assoc():
    pass
