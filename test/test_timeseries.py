from firebird import ARD
from firebird import ids
from firebird import timeseries

import json
import merlin
import pyspark.sql.types

dummy_list = [[5, 4, 3, 2], {"foo": 66}]
ard_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']
aux_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 'slope', 'mpw']
tile_resp  = json.loads(open("test/data/tile_response.json").read())
chip_ids   = ((-1815585.0, 1064805.0), (-1815585.0, 1061805.0), (-1815585.0, 1058805.0))

def test_schema_invalid():
    inv_schema = timeseries.schema("foo")
    assert inv_schema is None

def test_schema_ard():
    ardschema = timeseries.schema("ard")
    assert ardschema.names == ard_schema

def test_schema_aux():
    auxschema = timeseries.schema("aux")
    assert auxschema.names == aux_schema

def test_converter():
    converter = timeseries.converter()
    assert converter(dummy_list) == {"chipx": 5, "chipy": 4, "x": 3, "y": 2, "foo": 66}

def test_dataframe(spark_context):
    rdd = spark_context.parallelize(dummy_list)
    rdd.setName("ard")
    dframe = timeseries.dataframe(spark_context, rdd)
    assert type(dframe) is pyspark.sql.dataframe.DataFrame

def test_rdd(spark_context, monkeypatch):
    def mock_merlin_create(x, y, acquired, cfg):
        return [[[11, 22, 33, 44], (x, y)]]

    # timeseries.rdd depends on merlin.create, which calls configured chipmunk. don't do that.
    monkeypatch.setattr(merlin, 'create', mock_merlin_create)

    cids  = ids.rdd(ctx=spark_context, cids=chip_ids)
    ard   = timeseries.rdd(ctx=spark_context, cids=cids, acquired='1980-01-01/2017-01-01', cfg=ARD, name='ard')
    first = list(ard.map(lambda x: x).collect())

    assert type(ard) is pyspark.rdd.RDD
    assert first == [((11, 22, 33, 44), (-1815585.0, 1064805.0)), 
                     ((11, 22, 33, 44), (-1815585.0, 1061805.0)),
                     ((11, 22, 33, 44), (-1815585.0, 1058805.0))]

