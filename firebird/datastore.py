from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import firebird as fb


def connect():
    '''Creates a connection to a Cassandra cluster.'''
    auth_provider = PlainTextAuthProvider(username=fb.CASSANDRA_USER,
                                          password=fb.CASSANDRA_PASS)
    cluster = Cluster(fb.CASSANDRA_CONTACT_POINTS.split(','),
                      auth_provider=auth_provider)

    return cluster.connect()


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
             "result_md5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(fb.CASSANDRA_KEYSPACE, fb.CASSANDRA_RESULTS_TABLE)


def execute(statement, args, connection):
    try:
        for _ag in args:
            prepared = connection.prepare(statement)
            connection.execute(prepared, _ag)
    except Exception as e:
        raise e
    return True


def read(chip_x, chip_y, ulx, uly, lrx, lry, algorithm, datestring):
    """ Loads all algorithm results from the datastore and returns them
    with location information.
    :param ulx: Upper left x coordinate
    :param uly: Upper left y coordinate
    :param lyx: Lower right x coordinate
    :param lry: Lower right y coordinate
    :param algorithm: Name of algorithm
    :param datestring: String with date or date range
    :return:
    ((x1, y1, algorithm, datestring): algorithm_results,
     (x1, y2, algorithm, datestring): algorithm_results,)
    """
    pass


def save(chip_x, chip_y, x, y, algorithm, product_date, result):
    # format the results
    # coerce values
    # save to cassandra

    # Cassandra products keyspace
    # Tables need to be by H&V for tiles.  H1V1.  Need tile specs to calculate.
    # key needs to be chip_x, chip_y, x, y, algorithm, date (as string)
    # date as string, can be either the date of the product (date of map)
    # or the acquired date range for ccd results or wahtever else.
    # Must have it though to be able to store date based products

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
