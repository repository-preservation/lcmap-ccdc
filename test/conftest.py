from ccdc        import grid
from ccdc        import ids
from ccdc        import timeseries
from copy        import deepcopy
from pyspark     import SparkContext
from pyspark.sql import SparkSession, SQLContext

import ccdc
import pytest
import test

def get_chip_ids_rdd(chipids):
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return ids.rdd(ctx=sc, xys=chipids)

@pytest.fixture()
def ids_rdd():
    return get_chip_ids_rdd(((-1815585.0, 1064805.0), (-1815585.0, 1061805.0), (-1815585.0, 1058805.0)))

@pytest.fixture()
def merlin_ard_config():
    cfg = deepcopy(ccdc.ARD)
    cfg['near_fn'] = test.merlin_near_partial
    cfg['grid_fn'] = test.merlin_grid_partial
    cfg['snap_fn'] = test.merlin_snap_partial
    cfg['registry_fn'] = test.merlin_regy_partial
    cfg['chips_fn'] = test.merlin_chip_partial
    cfg['format_fn'] = test.merlin_frmt_partial
    return cfg

@pytest.fixture()
def merlin_aux_config():
    cfg = deepcopy(ccdc.AUX)
    cfg['near_fn'] = test.merlin_near_partial
    cfg['grid_fn'] = test.merlin_grid_partial
    cfg['snap_fn'] = test.merlin_snap_partial
    return cfg

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
    cids   = ids.rdd(ctx=sc, xys=chips)
    return timeseries.rdd(ctx=sc, cids=cids, acquired='1980-01-01/2017-01-01', cfg=config, name='ard')    

