import firebird as fb


def cassandra(table, dataframe, mode='append'):
    """Write a dataframe to Cassandra
    :param table: table name
    :param mode: append, overwrite, error, ignore
    :param dataframe: A Spark DataFrame
    """
    # https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md

    return dataframe.write.format('org.apache.spark.sql.cassandra')\
                                  .mode(mode)\
                                  .options(**fb.cassandra_options(table)).save()

def s3(table, dataframe, mode='append'):
    pass
