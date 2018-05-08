from firebird import timeseries

import json
import pyspark.sql.types

dummy_list = [[5, 4, 3, 2], {"foo": 66}]

def test_schema_invalid():
    inv_schema = timeseries.schema("foo")
    assert inv_schema is None

def test_schema_ard():
    ard_schema = timeseries.schema("ard")
    assert ard_schema.names == ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']

def test_schema_aux():
    aux_schema = timeseries.schema("aux")
    assert aux_schema.names == ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 'slope', 'mpw']

def test_converter():
    converter = timeseries.converter()
    assert converter(dummy_list) == {"chipx": 5, "chipy": 4, "x": 3, "y": 2, "foo": 66}

def test_dataframe(spark_context):
    rdd = spark_context.parallelize(dummy_list)
    rdd.setName("ard")
    dframe = timeseries.dataframe(spark_context, rdd)
    assert type(dframe) is pyspark.sql.dataframe.DataFrame



