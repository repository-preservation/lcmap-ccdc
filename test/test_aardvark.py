from firebird.aardvark import ubids
from firebird.aardvark import byubid
from firebird.aardvark import dates
from firebird.aardvark import intersection
from firebird.aardvark import trim
from firebird.aardvark import to_numpy
from firebird.aardvark import rods
from firebird.aardvark import locrods
import firebird as fb
from data import chip_specs

from base64 import b64encode
from functools import reduce
from functools import partial
from itertools import product

import numpy as np
from numpy.random import randint


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
    specs_byubid = byubid(specs)

    # run assertions
    checker = partial(_check, specs_byubid=specs_byubid)
    all(map(checker, to_numpy(chips, specs_byubid)))


def test_rods():
    chips = list()
    chips.append({'data': np.int_([[11, 12, 13],
                                   [14, 15, 16],
                                   [17, 18, 19]])})
    chips.append({'data': np.int_([[21, 22, 23],
                                   [24, 25, 26],
                                   [27, 28, 29]])})
    chips.append({'data': np.int_([[31, 32, 33],
                                   [34, 35, 36],
                                   [37, 38, 39]])})
    pillar = rods(chips)
    assert pillar.shape[0] == chips[0]['data'].shape[0]
    assert pillar.shape[1] == chips[0]['data'].shape[1]
    assert pillar.shape[2] == len(chips)

    # going to flatten both the chips arrays and the pillar array
    # then perform black magic and verify that the values wound up
    # where they belonged in the array positions.
    # using old style imperative code as double insurance
    # happiness is not doing this and relying on functional principles only
    # because direct manipulation of memory locations is error prone and
    # very difficult to think about.
    # We're mapping array locations between two things like that look like this:
    # [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 ...]
    # [11, 21, 31, 12, 22, 32, 13, 23, 33, 14, 24, 34, 15, 25, 35, 16, 26 ...]
    # This alone should serve as all the evidence needed to prove that
    # imperative programming is bad for everyone involved.  I am very sorry.
    fchips = list(fb.flatten([c['data'].flatten() for c in chips]))
    jump = reduce(lambda accum, v: accum + v, pillar.shape) # 9
    modulus = pillar.shape[0] # 3
    for i, val in enumerate(pillar.flatten()):
        factor = i % modulus # 0 1 2
        group = i // modulus # 0 1 2
        assert val == fchips[(factor * jump) + group]


def test_locrods():
    # test data.  sum(locs) + 2 should equal sum(rods) per inner array
    # value of sum of every inner array element should be unique.
    locs = np.int_([[[0, 0], [0, 1], [0, 2]],
                    [[1, 3], [1, 4], [1, 5]],
                    [[2, 6], [2, 7], [2, 8]]])

    rods = np.int_([[[0, 0, 2], [0, 0, 3], [0, 0, 4]],
                    [[1, 0, 5], [1, 0, 6], [1, 0, 7]],
                    [[2, 0, 8], [2, 0, 9], [2, 0, 10]]])

    # sanity check
    locs_total = locs.reshape(9, 2).sum(axis=1) + 2
    rods_total = rods.reshape(9, 3).sum(axis=1)
    assert np.array_equal(locs_total, rods_total)

    # make sure we located all the rods correctly.
    flatlocs = locs.reshape(9, 2)
    tseries = locrods(locs, rods)
    assert all([tseries[tuple(loc)].sum() == loc.sum() + 2 for loc in flatlocs])
