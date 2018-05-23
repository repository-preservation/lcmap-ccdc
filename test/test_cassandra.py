from firebird import cassandra

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

def test_read():
    assert True

def test_write():
    assert True
