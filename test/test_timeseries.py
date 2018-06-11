from ccdc import ARD
from ccdc import timeseries
from cytoolz  import first
from cytoolz  import second

from .shared import acquired
from .shared import ard_schema
from .shared import aux_schema
from .shared import mock_merlin_create
from .shared import mock_timeseries_rdd

import merlin
import pyspark.sql.types

def test_schema_invalid():
    assert timeseries.schema("foo") is None

def test_schema_ard():
    assert timeseries.schema("ard").names == ard_schema

def test_schema_aux():
    assert set(timeseries.schema("aux").names) == set(aux_schema)

def test_converter():
    converter = timeseries.converter()
    assert converter([[5, 4, 3, 2], {"foo": 66}]) == {"chipx": 5, "chipy": 4, "x": 3, "y": 2, "foo": 66}

def test_dataframe(spark_context):
    rdd = spark_context.parallelize([[5, 4, 3, 2], {"foo": 66}])
    rdd.setName("ard")
    dframe = timeseries.dataframe(spark_context, rdd)
    assert type(dframe) is pyspark.sql.dataframe.DataFrame

def test_rdd(spark_context, ids_rdd, merlin_ard_config):
    ard   = timeseries.rdd(ctx=spark_context, cids=ids_rdd, acquired=acquired, cfg=merlin_ard_config, name='ard')
    results = ard.map(lambda x: x).collect()
    assert type(ard) is pyspark.rdd.RDD
    assert type(first(results)) is tuple #((11, 22, 33, 44), (-1815585.0, 1064805.0)) 
    assert len(first(results)) == 2
    assert type(first(results)[1][0]) is float

def test_ard(spark_context, ids_rdd, merlin_ard_config):
    ard_df = timeseries.ard(spark_context, ids_rdd, acquired, cfg=merlin_ard_config)
    assert type(ard_df) is pyspark.sql.dataframe.DataFrame
    assert ard_df.columns == ard_schema

def test_aux(spark_context, ids_rdd, merlin_aux_config):
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired, cfg=merlin_aux_config)
    assert type(aux_df) is pyspark.sql.dataframe.DataFrame
    assert set(aux_df.columns) == set(aux_schema)
    
