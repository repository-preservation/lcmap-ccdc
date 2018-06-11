from ccdc    import cassandra
from ccdc    import pyccd
from pyspark import sql


def test_options():
    opts = cassandra.options("foo")
    assert set(opts.keys()) == {'table',
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

    
def test_read_write(ctx, timeseries_rdd):

    # create a dataframe from an rdd
    rdd       = ctx.parallelize([(100, -100, 200, -200, 33, 44), (300, -300, 400, -400, 55, 66)])
    layers    = rdd.map(lambda x: sql.Row(chipx=x[0], chipy=x[1], x=x[2], y=x[3], sday=x[4], eday=x[5]))
    sctx      = sql.SQLContext(ctx)
    dataframe = sctx.createDataFrame(layers)

    # write the dataframe to cassandra. cassandra.write returns NoneType, not a dataframe
    cassandra.write(ctx, dataframe, 'data')

    # read the table into a dataframe
    read_dataframe = cassandra.read(spark_context, 'data')
    assert set([i.asDict()["chipx"] for i in read_dataframe.collect()]) == set([100, 300])


