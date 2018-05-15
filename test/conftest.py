import pytest

from firebird import ids

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext

def get_chip_ids_rdd(chipids):
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return ids.rdd(ctx=sc, cids=chipids)

@pytest.fixture()
def spark_session():
    return SparkSession(SparkContext.getOrCreate())

@pytest.fixture()
def spark_context():
    return SparkSession(SparkContext.getOrCreate()).sparkContext

@pytest.fixture()
def sql_context():
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return SQLContext(sc)

@pytest.fixture()
def ids_rdd():
    return get_chip_ids_rdd(((-1815585.0, 1064805.0), (-1815585.0, 1061805.0), (-1815585.0, 1058805.0)))
