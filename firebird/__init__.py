import os
import sys
import logging
import numpy as np
import functools
import itertools
from datetime import datetime, date
from pyspark import SparkConf, SparkContext

AARDVARK = os.getenv('AARDVARK', 'http://localhost:5678')
AARDVARK_SPECS = os.getenv('AARDVARK_SPECS', '/v1/landsat/chip-specs')
SPECS_URL = ''.join([AARDVARK, AARDVARK_SPECS])
AARDVARK_CHIPS = os.getenv('AARDVARK_CHIPS', '/v1/landsat/chips')
CHIPS_URL = ''.join([AARDVARK, AARDVARK_CHIPS])

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

QA_BIT_PACKED              = os.getenv('CCD_QA_BITPACKED', 'True')

# TODO: This needs to be passed in from the command line
BEGINNING_OF_TIME         = os.getenv('BEGINNING_OF_TIME', date(year=1982, month=1, day=1).toordinal())

# TODO: These are obtained from chip specs
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


def sparkcon():
    try:
        conf = (SparkConf().setAppName("lcmap-firebird-{}".format(datetime.now().strftime('%Y-%m-%d-%I:%M')))
                .setMaster(SPARK_MASTER)
                .set("spark.mesos.executor.docker.image", SPARK_EXECUTOR_IMAGE)
                .set("spark.executor.cores", SPARK_EXECUTOR_CORES)
                .set("spark.mesos.executor.docker.forcePullImage", SPARK_EXECUTOR_FORCE_PULL))
        return SparkContext(conf=conf)
    except Exception as e:
        logger.info("Exception creating SparkContext: {}".format(e))
        raise e


def ccd_params():
    _p = {}
    if QA_BIT_PACKED is not 'True':
        _p = {'QA_BITPACKED': False,
              'QA_FILL': 255,
              'QA_CLEAR': 0,
              'QA_WATER': 1,
              'QA_SHADOW': 2,
              'QA_SNOW': 3,
              'QA_CLOUD': 4}
    return _p


def minbox(points):
    """ Returns the minimal bounding box necessary to contain points """
    pass


def dtstr_to_ordinal(dtstr):
    """ Return ordinal from string formatted date"""
    _dt = datetime.strptime(dtstr.split('T' if 'T' in dtstr else ' ')[0], '%Y-%m-%d')
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


def rsort(iterable, key=None):
    """Reverse sorts an iterable"""
    return sorted(iterable, key=key, reverse=True)


def compose(*functions):
    def compose2(f, g):
        return lambda x: f(g(x))
    return functools.reduce(compose2, functions, lambda x: x)


def flatten(iterable):
    """
    Reduce dimensionality of iterable containing iterables
    :param iterable: A multi-dimensional iterable
    :returns: A one dimensional iterable
    """
    return itertools.chain.from_iterable(iterable)


def flattend(dicts):
    """ Combines a sequence of dicts into a single dict.
    :params dicts: A sequence of dicts
    :returns: A single dict"""
    return {k:v for d in dicts for k, v in d.items()}

    
