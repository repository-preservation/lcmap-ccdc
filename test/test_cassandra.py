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
    rdd       = spark_context.parallelize([('lc08_srb1', 'foo_srb1.tif', '', '005015', '', ''),
                                           ('lc08_srb2', 'foo_srb2.tif', '', '005015', '', '')])
    layers    = rdd.map(lambda x: spark_sql.Row(layer=x[0], source=x[1], url=x[2], tile=x[3], chips=x[4], extra=x[5]))
    context   = spark_sql.SQLContext(spark_context)
    dataframe = context.createDataFrame(layers)
    # write the dataframe to cassandra. cassandra.write returns NoneType, not a dataframe
    cassandra.write(spark_context, dataframe, 'inventory')
    # read the table into a dataframe
    read_dataframe = cassandra.read(spark_context, 'inventory')

    assert set(read_dataframe.columns) == set(['layer', 'source', 'url', 'tile', 'chips', 'extra'])
    assert set([i.asDict()["layer"] for i in read_dataframe.collect()]) == set(['lc08_srb1', 'lc08_srb2'])


