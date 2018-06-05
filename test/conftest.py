import pytest
import firebird

from copy        import deepcopy
from firebird    import grid
from firebird    import ids
from firebird    import timeseries
from pyspark     import SparkContext
from pyspark.sql import SparkSession, SQLContext
from .shared     import merlin_grid_partial
from .shared     import merlin_near_partial
from .shared     import merlin_snap_partial
from .shared     import merlin_regy_partial
from .shared     import merlin_chip_partial
from .shared     import merlin_frmt_partial

def get_chip_ids_rdd(chipids):
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return ids.rdd(ctx=sc, cids=chipids)

@pytest.fixture()
def ids_rdd():
    return get_chip_ids_rdd(((-1815585.0, 1064805.0), (-1815585.0, 1061805.0), (-1815585.0, 1058805.0)))

@pytest.fixture()
def merlin_ard_config():
    mock_cfg = deepcopy(firebird.ARD)
    mock_cfg['near_fn'] = merlin_near_partial
    mock_cfg['grid_fn'] = merlin_grid_partial
    mock_cfg['snap_fn'] = merlin_snap_partial
    mock_cfg['registry_fn'] = merlin_regy_partial
    mock_cfg['chips_fn'] = merlin_chip_partial
    mock_cfg['format_fn'] = merlin_frmt_partial
    return mock_cfg

@pytest.fixture()
def merlin_aux_config():
    mock_cfg = deepcopy(firebird.AUX)
    mock_cfg['near_fn'] = merlin_near_partial
    mock_cfg['grid_fn'] = merlin_grid_partial
    mock_cfg['snap_fn'] = merlin_snap_partial
    return mock_cfg

@pytest.fixture()
def spark_context():
    return SparkSession(SparkContext.getOrCreate()).sparkContext

@pytest.fixture()
def sql_context():
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return SQLContext(sc)

@pytest.fixture()
def timeseries_rdd():
    sc     = spark_context()
    config = merlin_ard_config()
    tile   = grid.tile(100, 200, config)
    chips  = grid.chips(tile) # chips == [(-543585, 2378805)]
    cids   = ids.rdd(ctx=sc, cids=chips)
    return timeseries.rdd(ctx=sc, cids=cids, acquired='1980-01-01/2017-01-01', cfg=config, name='ard')    

