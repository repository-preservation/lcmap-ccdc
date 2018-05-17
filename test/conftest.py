import pytest
import firebird

from copy        import deepcopy
from firebird    import ids
from pyspark     import SparkContext
from pyspark.sql import SparkSession, SQLContext
from .shared     import merlin_grid_partial
from .shared     import merlin_near_partial
from .shared     import merlin_snap_partial

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


