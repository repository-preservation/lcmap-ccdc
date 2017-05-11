from firebird.cassandra import execute, INSERT_CQL
from datetime import datetime

_d = datetime.strptime('2014-07-30T06:51:36Z', '%Y-%m-%dT%H:%M:%SZ')


def test_save_detect():
    # in travis-ci, cant yet connect to container running cassandra
    results = {'y': 999888, 'result_ok': True, 'algorithm': 'pyccd-1.1.0', 'inputs_md5': 'xoxoxo', 'chip_x':-123456,
               'result': 'a pile of result', 'result_produced': _d, 'result_md5': 'result_md5',
               'x': -134567, 'chip_y': 888999}
    assert execute(INSERT_CQL, [results])

