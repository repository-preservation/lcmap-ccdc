import firebird as fb

logger = logging.getLogger(__name__)


def opts(table):
    # https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md
    return {
        'table': table,
        'keyspace': fb.CASSANDRA_KEYSPACE,
        'spark.cassandra.auth.username': fb.CASSANDRA_USER,
        'spark.cassandra.auth.password': fb.CASSANDRA_PASS,
        'spark.cassandra.connection.compression': 'LZ4',
        'spark.cassandra.connection.host': fb.CASSANDRA_CONTACT_POINTS,
        'spark.cassandra.input.consistency.level': 'QUORUM',
        'spark.cassandra.output.consistency.level': 'QUORUM'
    }


def read(table):
    """Read a dataframe from Cassandra"""
    return spark.read.format("org.apache.spark.sql.cassandra")\
               .options(**opts(table)).load()


def write(table, dataframe, mode='append'):
    """Write a dataframe to Cassandra

    Args:
        table (str): table name
        mode (str): append, overwrite, error, ignore
        dataframe: A Spark DataFrame

    Returns:
        The Spark DataFrame from the dataframe argument.
    """

    dataframe.write.format('org.apache.spark.sql.cassandra')\
        .mode(mode).options(**opts(table)).save()

    return dataframe
