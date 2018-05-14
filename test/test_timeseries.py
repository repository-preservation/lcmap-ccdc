from firebird    import ARD
from firebird    import ids
from firebird    import timeseries
from pyspark     import SparkContext
from pyspark.sql import SparkSession

from test.shared import acquired
from test.shared import ard_schema
from test.shared import aux_schema
from test.shared import dummy_list
from test.shared import mock_merlin_create
from test.shared import mock_timeseries_rdd

import json
import merlin
import pyspark.sql.types

def test_schema_invalid():
    assert timeseries.schema("foo") is None

def test_schema_ard():
    assert timeseries.schema("ard").names == ard_schema

def test_schema_aux():
    assert timeseries.schema("aux").names == aux_schema

def test_converter():
    converter = timeseries.converter()
    assert converter(dummy_list) == {"chipx": 5, "chipy": 4, "x": 3, "y": 2, "foo": 66}

def test_dataframe(spark_context):
    rdd = spark_context.parallelize(dummy_list)
    rdd.setName("ard")
    dframe = timeseries.dataframe(spark_context, rdd)
    assert type(dframe) is pyspark.sql.dataframe.DataFrame

def test_rdd(spark_context, monkeypatch, ids_rdd):
    monkeypatch.setattr(merlin, 'create', mock_merlin_create)
    ard   = timeseries.rdd(ctx=spark_context, cids=ids_rdd, acquired=acquired, cfg=ARD, name='ard')
    first = list(ard.map(lambda x: x).collect())

    assert type(ard) is pyspark.rdd.RDD
    assert first == [((11, 22, 33, 44), (-1815585.0, 1064805.0)), 
                     ((11, 22, 33, 44), (-1815585.0, 1061805.0)),
                     ((11, 22, 33, 44), (-1815585.0, 1058805.0))]

def test_ard(spark_context, monkeypatch, ids_rdd):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)
    ard_df = timeseries.ard(spark_context, ids_rdd, acquired)

    assert type(ard_df) is pyspark.sql.dataframe.DataFrame
    assert ard_df.columns == ard_schema

def test_aux(spark_context, monkeypatch, ids_rdd):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired)

    assert type(aux_df) is pyspark.sql.dataframe.DataFrame
    assert aux_df.columns == aux_schema
    
