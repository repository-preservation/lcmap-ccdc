import firebird as fb

# https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md
# must filter the returned dataframe
# with predicate pushdown set to true in the options (default is true)
# filters will be pushed to cassandra instead of happening in spark.
# There are restrictions.  Read the link.
def read(table):
 return spark.read.format("org.apache.spark.sql.cassandra")\
    .options(**fb.cassandra_options(table))\
    .load()
