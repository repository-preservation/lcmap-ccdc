from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from firebird import CASSANDRA_CONTACT_POINTS
from firebird import CASSANDRA_USER
from firebird import CASSANDRA_PASS
from firebird import CASSANDRA_KEYSPACE
from firebird import CASSANDRA_RESULTS_TABLE

from datetime import datetime

auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster(CASSANDRA_CONTACT_POINTS.split(','), auth_provider=auth_provider)

RESULT_INPUT = {'chip_x': int(),
                'chip_y': int(),
                'x': '',
                'y': '',
                'algorithm': '',
                'result': '',
                'result_ok': '',
                'result_produced': datetime,
                'inputs_md5': '',
                'result_md5': ''}

INSERT_CQL = "INSERT INTO {}.{} (y, chip_x, chip_y, algorithm, x, result_ok, inputs_md5, result, result_produced, " \
             "result_md5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(CASSANDRA_KEYSPACE, CASSANDRA_RESULTS_TABLE)


def execute(statement, args):
    try:
        with cluster.connect() as session:
            for _ag in args:
                prepared = session.prepare(statement)
                session.execute(prepared, _ag)
    except Exception as e:
        raise e

    return True


def save(chip_x, chip_y, x, y, algorithm, results, result_ok=True):
    # format the results
    # coerce values
    # save to cassandra

    """ Execute ccd.detect """
    output = cass.RESULT_INPUT.copy()
    output['chip_x'] = int(chip_x)
    output['chip_y'] = int(chip_y)
    output['x'] = int(x)
    output['y'] = int(y)
    output['result_produced'] = datetime.now()
    output['inputs_md5'] = 'not implemented'
    try:
        _results = ccd.detect(dates=bands['dates'],
                              blues=bands['blues'],
                              greens=bands['greens'],
                              reds=bands['reds'],
                              nirs=bands['nirs'],
                              swir1s=bands['swir1s'],
                              swir2s=bands['swir2s'],
                              thermals=bands['thermals'],
                              quality=bands['quality'],
                              params=fb.ccd_params())
        output['result'] = json.dumps(simplify_detect_results(_results))
        output['result_ok'] = True
        output['algorithm'] = _results['algorithm']
    except Exception as e:
        fb.logger.error("Exception running ccd.detect: {}".format(e))
        output['result'] = ''
        output['result_ok'] = False

    output['result_md5'] = hashlib.md5(output['result'].encode('UTF-8')).hexdigest()

    # writes to cassandra happen from node doing the work
    # don't want to collect all chip records on driver host
    #cass.execute(cass.INSERT_CQL, [output])
    return output
