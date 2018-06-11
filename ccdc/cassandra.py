from cytoolz import assoc
from pyspark.sql import SparkSession
import ccdc

def options(table):
    """Returns Cassandra Spark Connector options

    Args:
        table (str): The table to generate options for

    Returns:
        dict: Python dictionary of Cassandra options
    """
    
    return {
        'table': table,
        'keyspace': ccdc.keyspace(),
        'spark.cassandra.auth.username': ccdc.CASSANDRA_USER,
        'spark.cassandra.auth.password': ccdc.CASSANDRA_PASS,
        'spark.cassandra.connection.compression': 'LZ4',
        'spark.cassandra.connection.host': ccdc.CASSANDRA_HOST,
        'spark.cassandra.connection.port': ccdc.CASSANDRA_PORT,
        'spark.cassandra.input.consistency.level': ccdc.CASSANDRA_INPUT_CONSISTENCY_LEVEL,
        'spark.cassandra.output.consistency.level': ccdc.CASSANDRA_OUTPUT_CONSISTENCY_LEVEL,
        'spark.cassandra.output.concurrent.writes': ccdc.CASSANDRA_OUTPUT_CONCURRENT_WRITES,
        'spark.cassandra.output.batch.grouping.buffer.size': 500
    }


def read(sc, table):
    """Read data from Cassandra as a dataframe

    Args:
        sc: spark context
        table: cassandra table to read from

    Returns:
        dataframe
    """
    
    opts = options(table)
    return SparkSession(sc).read.format('org.apache.spark.sql.cassandra').options(**opts).load()


def write(sc, dataframe, table):
    """Write a dataframe to cassandra using options.  

    Dataframe must conform to the table schema.
    
    Args:
        sc: spark context
        dataframe: The dataframe to write
        table: Cassandra table to write dataframe to

    Returns:
        dataframe
    """

    opts = options(table)
    msg  = assoc(opts, 'spark.cassandra.auth.password', 'XXXXX')
    ccdc.logger(sc, name=__name__).info('writing dataframe:{}'.format(msg))
    return dataframe.write.format('org.apache.spark.sql.cassandra')\
                          .mode('append').options(**opts).save()
