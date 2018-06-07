from firebird import cassandra
from firebird import pyccd

import pyspark.sql as spark_sql

def test_options():
    options_resp = cassandra.options("foo")
    assert set(options_resp.keys()) == {'table',
                                        'keyspace',
                                        'spark.cassandra.auth.password', 
                                        'spark.cassandra.auth.username', 
                                        'spark.cassandra.connection.compression', 
                                        'spark.cassandra.connection.host', 
                                        'spark.cassandra.connection.port', 
                                        'spark.cassandra.input.consistency.level',
                                        'spark.cassandra.output.batch.grouping.buffer.size', 
                                        'spark.cassandra.output.concurrent.writes', 
                                        'spark.cassandra.output.consistency.level'}

def test_read_write(spark_context, timeseries_rdd):
    # create a dataframe from an rdd
    rdd       = spark_context.parallelize([(100, -100, 200, -200, 33, 44),
                                           (300, -300, 400, -400, 55, 66)])
    layers    = rdd.map(lambda x: spark_sql.Row(chipx=x[0], chipy=x[1], x=x[2], y=x[3], sday=x[4], eday=x[5]))
    context   = spark_sql.SQLContext(spark_context)
    dataframe = context.createDataFrame(layers)
    # write the dataframe to cassandra. cassandra.write returns NoneType, not a dataframe
    cassandra.write(spark_context, dataframe, 'lcmap_pyccd_2018_03_12')
    # read the table into a dataframe
    read_dataframe = cassandra.read(spark_context, 'lcmap_pyccd_2018_03_12')
    assert set([i.asDict()["chipx"] for i in read_dataframe.collect()]) == set([100, 300])


