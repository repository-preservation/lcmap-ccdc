from firebird import functions as f
from firebird import rods as fr
from functools import reduce
import numpy as np

def test_from_chips():
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
    pillar = fr.from_chips(chips)
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
    fchips = list(f.flatten([c['data'].flatten() for c in chips]))
    jump = reduce(lambda accum, v: accum + v, pillar.shape) # 9
    modulus = pillar.shape[0] # 3
    for i, val in enumerate(pillar.flatten()):
        factor = i % modulus # 0 1 2
        group = i // modulus # 0 1 2
        assert val == fchips[(factor * jump) + group]


def test_locate():
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
    tseries = fr.locate(locs, rods)
    assert all([tseries[tuple(loc)].sum() == loc.sum() + 2 for loc in flatlocs])
