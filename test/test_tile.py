from ccdc import tile
from pyspark.sql import Row

import datetime
import json
import test

rows = [Row(tx=0,
            ty=1,
            name='test model',
            model='model coefficients',
            updated='0001-01-01')]


def test_table():
    assert 'tile' == tile.table()


def test_schema():
    s = tile.schema().simpleString()
    print(s)
    assert s == 'struct<tx:int,ty:int,name:string,model:string,updated:string>'

    
def test_dataframe(spark_context, sql_context):
    df   = sql_context.createDataFrame(rows)
    tdf  = tile.dataframe(spark_context, tx=0,
                                         ty=1,
                                         name='test model',
                                         model='model coefficients',
                                         updated=datetime.date.fromordinal(1).isoformat()).toJSON().collect()
    assert tdf == ['{"model":"model coefficients","name":"test model","tx":0,"ty":1,"updated":"0001-01-01"}']

    
def test_read_write(spark_context, sql_context):
    ids = [Row(tx=0, ty=1)]
    idf = sql_context.createDataFrame(ids)
    df      = sql_context.createDataFrame(rows)
    tdf     = tile.dataframe(spark_context, tx=0, ty=1, name='test model', model='model coefficients', updated='0001-01-01')
    written = tile.write(spark_context, tdf).toJSON().collect()[0]
    read    = tile.read(spark_context, idf).toJSON().collect()[0]

    w = json.loads(written)
    r = json.loads(read)
    assert w == r
