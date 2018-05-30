from firebird import cassandra
from firebird import ids
from firebird import pyccd

from .shared import ccd_schema_names
from .shared import ccd_format_keys
from .shared import faux_dataframe
from .shared import mock_cassandra_read
from .shared import timeseries_element

from pyspark.sql.types import StructType
from pyspark.rdd import PipelinedRDD

def test_algorithm():
    assert "lcmap-pyccd" in pyccd.algorithm()

def test_table():
    assert "lcmap_pyccd_" in pyccd.table()

def test_schema():
    assert type(pyccd.schema()) is StructType
    assert set(pyccd.schema().names) == set(ccd_schema_names)

def test_dataframe(spark_context, timeseries_rdd):
    rdd    = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    dframe = pyccd.dataframe(spark_context, rdd)
    set(dframe.columns) == set(ccd_schema_names)

def test_default():
    assert pyccd.default([]) == [{'start_day': 0, 'end_day': 0}]
    assert pyccd.default(["foo", "bar"]) == ["foo", "bar"]

def test_format():
    ccdresult = {'change_models': []}
    pyccd_format = pyccd.format(-100, 200, -125, 195, "1980-01-01/2017-12-31", ccdresult)
    assert set(pyccd_format[0].keys()) == set(ccd_format_keys)

def test_detect():
    result = pyccd.detect(timeseries_element)[0]
    assert result['chipx'] == -1815585
    assert set(result.keys()) == set(ccd_format_keys)

def test_rdd(spark_context, timeseries_rdd):
    # calling collect, or any other method to realize the results fails
    # unless we monkeypatch the function which actually retrieves chip data.
    # not sure yet if thats beyond scope for this test or not.
    rdd = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    assert type(rdd) is PipelinedRDD

def test_read(monkeypatch, ids_rdd, spark_context, sql_context):
    monkeypatch.setattr(cassandra, 'read', mock_cassandra_read)
    ids_df = ids.dataframe(spark_context, ids_rdd)
    pyccd_read = pyccd.read(ctx=sql_context, ids=ids_df)
    assert set(['chipx','chipy','srb1']) == set(pyccd_read.schema.names)

def test_write(monkeypatch, sql_context):
    # excercises cassandra.write, returns dataframe passed as arg
    monkeypatch.setattr(cassandra, 'write', lambda a, b, c: True)
    df = faux_dataframe(sql_context, ['a', 'b', 'c'])
    pyccd_write = pyccd.write(sql_context, df)
    assert pyccd_write == df

def test_join(sql_context):
    df_attrs1 = ['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'rfrawp']
    df_attrs2 = ['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'srb3']
    ccd_df    = faux_dataframe(ctx=sql_context, attrs=df_attrs1)
    pred_df   = faux_dataframe(ctx=sql_context, attrs=df_attrs2)
    joined_df = pyccd.join(ccd=ccd_df, predictions=pred_df)
    assert set(['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'srb3']) == set(joined_df.schema.names)

