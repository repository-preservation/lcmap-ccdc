from firebird    import ARD
from firebird    import ids
from firebird    import timeseries
from pyspark     import SparkContext
from pyspark.sql import SparkSession

import json
import merlin
import pyspark.sql.types

def mock_timeseries_rdd(ctx, cids, acquired, cfg, name):
    rdd = ctx.parallelize([[[11, 22, 33, 44], (-999, 111)]])
    rdd.setName(name)
    return rdd

def mock_merlin_create(x, y, acquired, cfg):
    return [[[11, 22, 33, 44], (x, y)]]

def get_chip_ids_rdd(chipids):
    sc = SparkSession(SparkContext.getOrCreate()).sparkContext
    return ids.rdd(ctx=sc, cids=chipids)

dummy_list = [[5, 4, 3, 2], {"foo": 66}]
ard_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']
aux_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 'slope', 'mpw']
tile_resp  = json.loads(open("test/data/tile_response.json").read())
ids_rdd    = get_chip_ids_rdd(((-1815585.0, 1064805.0), (-1815585.0, 1061805.0), (-1815585.0, 1058805.0)))
acquired   = '1980-01-01/2017-01-01'


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

def test_rdd(spark_context, monkeypatch):
    monkeypatch.setattr(merlin, 'create', mock_merlin_create)
    ard   = timeseries.rdd(ctx=spark_context, cids=ids_rdd, acquired=acquired, cfg=ARD, name='ard')
    first = list(ard.map(lambda x: x).collect())

    assert type(ard) is pyspark.rdd.RDD
    assert first == [((11, 22, 33, 44), (-1815585.0, 1064805.0)), 
                     ((11, 22, 33, 44), (-1815585.0, 1061805.0)),
                     ((11, 22, 33, 44), (-1815585.0, 1058805.0))]

def test_ard(spark_context, monkeypatch):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)
    ard_df = timeseries.ard(spark_context, ids_rdd, acquired)

    assert type(ard_df) is pyspark.sql.dataframe.DataFrame
    assert ard_df.columns == ard_schema

def test_aux(spark_context, monkeypatch):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired)

    assert type(aux_df) is pyspark.sql.dataframe.DataFrame
    assert aux_df.columns == aux_schema
    
