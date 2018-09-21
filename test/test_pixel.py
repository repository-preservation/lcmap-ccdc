from ccdc import pixel
from pyspark.sql import Row

import datetime
import test


def test_table():
    assert 'pixel' == pixel.table()


def test_schema():
    s = pixel.schema().simpleString()
    assert s == 'struct<cx:int,cy:int,px:int,py:int,mask:array<tinyint>>'

    
def test_dataframe(spark_context, sql_context):
    rows = [Row(cx=0,
                cy=1,
                px=3,
                py=4,
                mask=[0, 1, 2, 3, 4],
                extra=True)]
    df   = sql_context.createDataFrame(rows)
    cdf  = pixel.dataframe(spark_context, df).toJSON().collect()
    assert cdf == ['{"cx":0,"cy":1,"px":3,"py":4,"mask":[0,1,2,3,4]}']

    
def test_read_write(spark_context, sql_context):
    ids = [Row(cx=0, cy=1)]
    idf = sql_context.createDataFrame(ids)

    rows = [Row(cx=0,
                cy=1,
                px=3,
                py=4,
                mask=[0, 1, 2, 3, 4])]
    df      = sql_context.createDataFrame(rows)
    pdf     = pixel.dataframe(spark_context, df)
    written = pixel.write(spark_context, pdf)
    read    = pixel.read(spark_context, idf)

    assert read.toJSON().collect() == written.toJSON().collect()

