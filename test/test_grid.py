from firebird import grid

from .shared import mock_merlin_grid
from .shared import mock_merlin_snap

import firebird
import merlin

def test_definition(monkeypatch):
    # all thats being tested is that merlin.chipmunk is the grid_fn
    # in firebird.ARD
    monkeypatch.setattr(merlin.chipmunk, 'grid', mock_merlin_grid)
    defn = grid.definition()[0]
    assert set(defn.keys()) == {'proj', 'tx', 'sy', 'ty', 'ry', 'rx', 'sx', 'name'}

def test_tile(monkeypatch):
    monkeypatch.setattr(merlin.chipmunk, 'grid', mock_merlin_grid)
    monkeypatch.setattr(merlin.chipmunk, 'snap', mock_merlin_snap)
    grid_tile = grid.tile(100, 200, firebird.ARD)
    assert set(grid_tile.keys()) == set(['x', 'y', 'h', 'v', 'ulx', 'uly', 'lrx', 'lry', 'chips'])
    assert len(grid_tile['chips']) == 2500

def test_chips():
    chips = grid.chips({"x": -100, "y": 100, "chips": [(1, 1), (2, 2)]})
    assert set(chips) == set([(1, 1), (2, 2)])

def test_training():
    assert True

def test_classification():
    assert True
