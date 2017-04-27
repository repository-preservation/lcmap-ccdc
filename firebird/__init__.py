import os
import sys
import logging
import numpy as np
from datetime import datetime, date

AARDVARK_HOST             = os.getenv('AARDVARK_HOST', 'localhost')
AARDVARK_PORT             = os.getenv('AARDVARK_PORT', '5678')
AARDVARK_SPECS            = os.getenv('AARDVARK_SPECS', '/landsat/chip-specs')
AARDVARK_SPECS_URL        = ''.join([AARDVARK_HOST, ':', AARDVARK_PORT, AARDVARK_SPECS])

CASSANDRA_CONTACT_POINTS  = os.getenv('CASSANDRA_CONTACT_POINTS', '0.0.0.0')
CASSANDRA_USER            = os.getenv('CASSANDRA_USER')
CASSANDRA_PASS            = os.getenv('CASSANDRA_PASS')
CASSANDRA_KEYSPACE        = os.getenv('CASSANDRA_KEYSPACE', 'lcmap_changes_local')
CASSANDRA_RESULTS_TABLE   = os.getenv('CASSANDRA_RESULTS_TABLE', 'results')

LOG_LEVEL                 = os.getenv('FIREBIRD_LOG_LEVEL', 'INFO')

SPARK_MASTER              = os.getenv('SPARK_MASTER', 'spark://localhost:7077')
SPARK_EXECUTOR_IMAGE      = os.getenv('SPARK_EXECUTOR_IMAGE')
SPARK_EXECUTOR_CORES      = os.getenv('SPARK_EXECUTOR_CORES', 1)
SPARK_EXECUTOR_FORCE_PULL = os.getenv('SPARK_EXECUTOR_FORCE_PULL', 'false')

BEGINNING_OF_TIME         = os.getenv('BEGINNING_OF_TIME', date(year=1982, month=1, day=1).toordinal())
X_PIXEL_DIM               = int(os.getenv('X_PIXEL_DIM', 30))
Y_PIXEL_DIM               = int(os.getenv('Y_PIXEL_DIM', -30))


logging.basicConfig(stream=sys.stdout,
                    level=LOG_LEVEL,
                    format='%(asctime)s %(module)s::%(funcName)-20s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# default all loggers to WARNING then explictly override below
logging.getLogger("").setLevel(logging.WARNING)

# let firebird.* modules use configuration value
logger = logging.getLogger('firebird')
logger.setLevel(LOG_LEVEL)


def minbox(points):
    """ Returns the minimal bounding box necessary to contain points """
    pass


def dtstr_to_ordinal(dtstr, iso=True):
    """ Return ordinal from string formatted date"""
    _fmt = '%Y-%m-%dT%H:%M:%SZ' if iso else '%Y-%m-%d %H:%M:%S'
    _dt = datetime.strptime(dtstr, _fmt)
    return _dt.toordinal()


def simplify_objects(obj):
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.int64):
        return int(obj)
    elif isinstance(obj, tuple) and ('_asdict' in dir(obj)):
        # looks like a namedtuple
        _out = {}
        objdict = obj._asdict()
        for key in objdict.keys():
            _out[key] = simplify_objects(objdict[key])
        return _out
    elif isinstance(obj, (list, np.ndarray, tuple)):
        return [simplify_objects(i) for i in obj]
    else:
        # should be a serializable type
        return obj


