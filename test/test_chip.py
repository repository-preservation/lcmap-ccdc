from firebird import chip
import numpy as np


def test_difference():
    assert chip.difference(3456, 3000) == 456
    assert chip.difference(3456, 5000) == 3456


def test_near():
    assert chip.near(2999, 3000, 0) == 0
    assert chip.near(3000, 3000, 0) == 3000
    assert chip.near(-2999, -3000, 0) == 0
    assert chip.near(-3000, -3000, 0) == -3000


def test_point_to_chip():
    assert chip.point_to_chip(2999, -2999, 3000, -3000, 0, 0) == (0, 0)
    assert chip.point_to_chip(3000, -3000, 3000, -3000, 0, 0) == (3000, -3000)


def test_snap():
    spec = {'chip_x': 3000, 'chip_y': -3000, 'shift_x': 0, 'shift_y': 0}
    assert (0, 0) == chip.snap(2999, -2999, spec)
    assert (3000, 0) == chip.snap(3000, -2999, spec)
    assert (0, -3000) == chip.snap(2999, -3000, spec)
    assert (3000, -3000) == chip.snap(3000, -3000, spec)


def test_ids():
    spec = {'chip_x': 3000, 'chip_y': -3000, 'shift_x': 0, 'shift_y': 0}
    ids  = set(((0, 0), (3000, -3000), (3000, 0), (0, -3000)))
    assert set(chip.ids(0, 0, 3000, -3000, spec)) == ids


def test_numpy():
    assert len("This is tested in test_aardvark:test_to_numpy()") > 0


def test_locations():
    spec = {'data_shape': (2, 2), 'pixel_x': 30, 'pixel_y': -30}
    locs = {(30, -30), (0, 0), (0, -30), (30, 0)}
    assert locs == set(map(tuple, chip.locations(0, 0, spec).reshape(4, 2)))
