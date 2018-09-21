from ccdc import cassandra
from ccdc import pyccd
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

    
def test_read_write(spark_context, timeseries_rdd):

    ctx       = spark_context
    # create a dataframe from an rdd
    rdd       = ctx.parallelize([(100, -100, 200, -200), (300, -300, 400, -400)])
    layers    = rdd.map(lambda x: sql.Row(cx=x[0], cy=x[1], px=x[2], py=x[3], sday='1999-01-01', eday='1999-12-31'))
    sctx      = sql.SQLContext(ctx)
    dataframe = sctx.createDataFrame(layers)

    # write the dataframe to cassandra. cassandra.write returns NoneType, not a dataframe
    cassandra.write(ctx, dataframe, 'segment')

    # read the table into a dataframe
    read_dataframe = cassandra.read(spark_context, 'segment')
    assert set([i.asDict()["cx"] for i in read_dataframe.collect()]) == set([100, 300])


