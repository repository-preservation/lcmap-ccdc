from base64 import b64encode
from firebird import chips as fc
from firebird import chip_specs as fcs
from functools import partial
from functools import reduce
from itertools import product
from numpy.random import randint
import numpy as np


def test_difference():
    assert fc.difference(3456, 3000) == 456
    assert fc.difference(3456, 5000) == 3456


def test_near():
    assert fc.near(2999, 3000, 0) == 0
    assert fc.near(3000, 3000, 0) == 3000
    assert fc.near(-2999, -3000, 0) == 0
    assert fc.near(-3000, -3000, 0) == -3000


def test_point_to_chip():
    assert fc.point_to_chip(2999, -2999, 3000, -3000, 0, 0) == (0, 0)
    assert fc.point_to_chip(3000, -3000, 3000, -3000, 0, 0) == (3000, -3000)


def test_snap():
    spec = {'chip_x': 3000, 'chip_y': -3000, 'shift_x': 0, 'shift_y': 0}
    assert (0, 0) == fc.snap(2999, -2999, spec)
    assert (3000, 0) == fc.snap(3000, -2999, spec)
    assert (0, -3000) == fc.snap(2999, -3000, spec)
    assert (3000, -3000) == fc.snap(3000, -3000, spec)


def test_ids():
    spec = {'chip_x': 3000, 'chip_y': -3000, 'shift_x': 0, 'shift_y': 0}
    ids  = set(((0, 0), (3000, -3000), (3000, 0), (0, -3000)))
    assert set(fc.ids(0, 0, 3000, -3000, spec)) == ids


def test_numpy():
    assert len("This is tested in test_aardvark:test_to_numpy()") > 0


def test_locations():
    spec = {'data_shape': (2, 2), 'pixel_x': 30, 'pixel_y': -30}
    locs = {(30, -30), (0, 0), (0, -30), (30, 0)}
    assert locs == set(map(tuple, fc.locations(0, 0, spec).reshape(4, 2)))


def test_dates():
    inputs = list()
    inputs.append({'acquired': '2015-04-01'})
    inputs.append({'acquired': '2017-04-01'})
    inputs.append({'acquired': '2017-01-01'})
    inputs.append({'acquired': '2016-04-01'})
    assert set(fc.dates(inputs)) == set(map(lambda d: d['acquired'], inputs))


def test_trim():
    inputs = list()
    inputs.append({'include': True, 'acquired': '2015-04-01'})
    inputs.append({'include': True, 'acquired': '2017-04-01'})
    inputs.append({'include': False, 'acquired': '2017-01-01'})
    inputs.append({'include': True, 'acquired': '2016-04-01'})
    included = fc.dates(filter(lambda d: d['include'] is True, inputs))
    trimmed = fc.trim(inputs, included)
    assert len(list(trimmed)) == len(included)
    assert set(included) == set(map(lambda x: x['acquired'], trimmed))


def test_to_numpy():
    """ Builds combos of shapes and numpy data types and tests
        aardvark.to_numpy() with all of them """

    def _ubid(dtype, shape):
        return dtype + str(shape)

    def _chip(dtype, shape, ubid):
        limits = np.iinfo(dtype)
        length = reduce(lambda accum, v: accum * v, shape)
        matrix = randint(limits.min, limits.max, length, dtype).reshape(shape)
        return {'ubid': ubid, 'data': b64encode(matrix)}

    def _spec(dtype, shape, ubid):
        return {'ubid': ubid, 'data_shape': shape, 'data_type': dtype.upper()}

    def _check(npchip, specs_byubid):
        spec = specs_byubid[npchip['ubid']]
        assert npchip['data'].dtype.name == spec['data_type'].lower()
        assert npchip['data'].shape == spec['data_shape']
        return True

    # Test combos of dtypes/shapes to ensure data shape and type are unaltered
    combos = tuple(product(('uint8', 'uint16', 'int8', 'int16'),
                           ((3, 3), (1, 1), (100, 100))))

    # generate the chip_specs and chips
    chips = [_chip(*c, _ubid(*c)) for c in combos]
    specs = [_spec(*c, _ubid(*c)) for c in combos]
    specs_byubid = fcs.byubid(specs)

    # run assertions
    checker = partial(_check, specs_byubid=specs_byubid)
    all(map(checker, fc.to_numpy(chips, specs_byubid)))
