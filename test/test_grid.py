from firebird  import grid

def test_definition(merlin_ard_config):
    # all thats being tested is that merlin.chipmunk is the grid_fn
    # in firebird.ARD
    defn = grid.definition(merlin_ard_config)[0]
    assert set(defn.keys()) == {'proj', 'tx', 'sy', 'ty', 'ry', 'rx', 'sx', 'name'}

def test_tile(merlin_ard_config):
    grid_tile = grid.tile(100, 200, merlin_ard_config)
    assert set(grid_tile.keys()) == set(['x', 'y', 'h', 'v', 'ulx', 'uly', 'lrx', 'lry', 'chips'])
    assert grid_tile['chips'] == ((-543585.0, 2378805.0),)

def test_chips():
    chips = grid.chips({"x": -100, "y": 100, "chips": [(1, 1), (2, 2)]})
    assert set(chips) == set([(1, 1), (2, 2)])

def test_training(merlin_aux_config):
    training_data = grid.training(100, 200, merlin_aux_config)
    assert len(training_data) == 9

def test_classification(merlin_aux_config):
    classification_chips = grid.classification(-100, 200, merlin_aux_config)
    assert classification_chips == [(-543585, 2378805)]
