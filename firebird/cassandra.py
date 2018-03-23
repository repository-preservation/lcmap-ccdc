from cytoolz import assoc
from pyspark.sql import SparkSession
import firebird

def options(table):
    """Returns Cassandra Spark Connector options

    Args:
        table (str): The table to generate options for

    Returns:
        dict: Python dictionary of Cassandra options
    """
    
    return {
        'table': table,
        'keyspace': firebird.CASSANDRA_KEYSPACE,
        'spark.cassandra.auth.username': firebird.CASSANDRA_USER,
        'spark.cassandra.auth.password': firebird.CASSANDRA_PASS,
        'spark.cassandra.connection.compression': 'LZ4',
        'spark.cassandra.connection.host': firebird.CASSANDRA_HOST,
        'spark.cassandra.connection.port': firebird.CASSANDRA_PORT,
        'spark.cassandra.input.consistency.level': firebird.CASSANDRA_INPUT_CONSISTENCY_LEVEL,
        'spark.cassandra.output.consistency.level': firebird.CASSANDRA_OUTPUT_CONSISTENCY_LEVEL,
        'spark.cassandra.output.concurrent.writes': firebird.CASSANDRA_OUTPUT_CONCURRENT_WRITES,
        'spark.cassandra.output.batch.grouping.buffer.size': 500
    }


def read(sc, table):
    opts = options(table)
    return SparkSession(sc).read.format('org.apache.spark.sql.cassandra').options(**opts).load()


def write(sc, dataframe, table):
    """Write a dataframe to cassandra using options.  

    Dataframe must conform to the table schema.
    
    Args:
        sc: Spark Context
        dataframe: The dataframe to write
        table: Cassandra table to write dataframe to

    Returns:
        dataframe
    """

    opts = options(table)
    msg  = assoc(opts, 'spark.cassandra.auth.password', 'XXXXX')
    firebird.logger(sc, name=__name__).info('writing dataframe:{}'.format(msg))
    return dataframe.write.format('org.apache.spark.sql.cassandra')\
                          .mode('append').options(**opts).save()
