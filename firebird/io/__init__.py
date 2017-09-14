cassandra_options = {
    'table': table,
    'keyspace': fb.CASSANDRA_KEYSPACE,
    'spark.cassandra.auth.username': fb.CASSANDRA_USER,
    'spark.cassandra.auth.password': fb.CASSANDRA_PASS,
    'spark.cassandra.connection.compression': 'LZ4',
    'spark.cassandra.connection.host': fb.CASSANDRA_CONTACT_POINTS,
    'spark.cassandra.input.consistency.level': 'QUORUM',
    'spark.cassandra.output.consistency.level': 'QUORUM'
}
