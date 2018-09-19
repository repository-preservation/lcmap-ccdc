from ccdc import chip

from pyspark.sql import Row
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

import datetime
import test


def test_table():
    assert 'chip' == chip.table()


def test_schema():
    s = chip.schema().simpleString()
    assert s == 'struct<cx:int,cy:int,dates:array<string>>'

    
def test_dataframe(spark_context, sql_context):
    rows = [Row(cx=0,
                cy=1,
                dates=[datetime.date.fromordinal(1).isoformat(),
                       datetime.date.fromordinal(2).isoformat()],
                extra=True)]
    df   = sql_context.createDataFrame(rows)
    cdf  = chip.dataframe(spark_context, df).toJSON().collect()
    assert cdf == ['{"cx":0,"cy":1,"dates":["0001-01-01","0001-01-02"]}']

    
def test_read_write(spark_context, sql_context):
    ids = [Row(cx=0, cy=1)]
    idf = sql_context.createDataFrame(ids)

    rows = [Row(cx=0,
                cy=1,
                dates=[datetime.date.fromordinal(1).isoformat(),
                       datetime.date.fromordinal(2).isoformat()])]
    df      = sql_context.createDataFrame(rows)
    cdf     = chip.dataframe(spark_context, df)
    written = chip.write(spark_context, cdf)
    read    = chip.read(spark_context, idf)

    print('Written:{}'.format(written.toJSON().collect()))

    print('-------------')
    
    print('Read:{}'.format(read.toJSON().collect()))


    assert read.toJSON().collect() == written.toJSON().collect()
    assert 1 < 0
