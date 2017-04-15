from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from firebird import CASSANDRA_CONTACT_POINTS
from firebird import CASSANDRA_USER
from firebird import CASSANDRA_PASS
from firebird import CASSANDRA_KEYSPACE

auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster(CASSANDRA_CONTACT_POINTS.split(','), auth_provider=auth_provider)


def save_pyccd_result(record):
    insrt_stmt = "INSERT INTO {}.results (y, tile_x, tile_y, algorithm, x, result_ok, inputs_md5, result, " \
                 "result_produced, result_md5) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(CASSANDRA_KEYSPACE)
    try:
        with cluster.connect() as session:
            session.execute(insrt_stmt, (record['y'], record['tile_x'], record['tile_y'], record['algorithm'], record['x'],
                                         record['result_ok'], record['inputs_md5'], record['result'],
                                         record['result_produced'], record['result_md5']))
        return True
    except Exception as e:
        raise Exception("Problem saving pyccd results to cassandra: {}".format(e))


