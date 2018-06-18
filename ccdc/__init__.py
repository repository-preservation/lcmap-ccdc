from pyspark import SparkConf
from pyspark import SparkContext
from urllib.parse import urlparse
import logging
import merlin
import multiprocessing
import os
import re
import socket

# All of these are evaluated at import time!!!  That means the env vars need
# to be set before the ccdc module is imported.  
HOST                               = socket.gethostbyname(socket.getfqdn())
ARD_CHIPMUNK                       = os.getenv('ARD_CHIPMUNK', 'http://localhost:5656')
AUX_CHIPMUNK                       = os.getenv('AUX_CHIPMUNK', 'http://localhost:5656')
CASSANDRA_HOST                     = os.getenv('CASSANDRA_URLS', HOST)
CASSANDRA_PORT                     = int(os.getenv('CASSANDRA_PORT', 9043))
CASSANDRA_USER                     = os.getenv('CASSANDRA_USER', 'cassandra')
CASSANDRA_PASS                     = os.getenv('CASSANDRA_PASS', 'cassandra')
CASSANDRA_OUTPUT_CONCURRENT_WRITES = int(os.getenv('CASSANDRA_OUTPUT_CONCURRENT_WRITES', 2))
CASSANDRA_OUTPUT_CONSISTENCY_LEVEL = os.getenv('CASSANDRA_OUTPUT_CONSISTENCY_LEVEL', 'QUORUM')
CASSANDRA_INPUT_CONSISTENCY_LEVEL  = os.getenv('CASSANDRA_INPUT_CONSISTENCY_LEVEL', 'QUORUM')
MESOS_PRINCIPAL                    = os.getenv('MESOS_PRINCIPAL', '')
MESOS_SECRET                       = os.getenv('MESOS_SECRET', '')
MESOS_ROLE                         = os.getenv('MESOS_ROLE', '')
IMAGE                              = os.getenv('IMAGE', '')
FORCE_PULL                         = os.getenv('FORCE_PULL', 'false')
SERIALIZER                         = os.getenv('SERIALIZER', 'org.apache.spark.serializer.KryoSerializer')
PYTHON_WORKER_MEMORY               = os.getenv('PYTHON_WORKER_MEMORY', '1g')
CORES_MAX                          = os.getenv('CORES_MAX', multiprocessing.cpu_count())
EXECUTOR_MEMORY                    = os.getenv('EXECUTOR_MEMORY', '4g')
INPUT_PARTITIONS                   = int(os.getenv('INPUT_PARTITIONS', 1))
PRODUCT_PARTITIONS                 = int(os.getenv('PRODUCT_PARTITIONS', multiprocessing.cpu_count() * 8))
ARD                                = merlin.cfg.get(profile='chipmunk-ard', env={'CHIPMUNK_URL': ARD_CHIPMUNK}) 
AUX                                = merlin.cfg.get(profile='chipmunk-aux', env={'CHIPMUNK_URL': AUX_CHIPMUNK}) 
TRAINING_SDAY                      = os.getenv('TRAINING_SDAY', 0)
TRAINING_EDAY                      = os.getenv('TRAINING_EDAY', 0)

def keyspace():
    """ Compute the CCDC keyspace.
        
        This value should always be a function of the URL for ARD, the URL for AUX
        and the CCDC version.

        Returns:
            The CCDC keyspace name
    """

    ard = re.sub("/", "", urlparse(ARD_CHIPMUNK).path)
    aux = re.sub("/", "", urlparse(AUX_CHIPMUNK).path)
    pwd = os.path.dirname(os.path.realpath(__file__))
    ver = merlin.files.read('{}{}version.txt'.format(os.path.dirname(pwd), os.path.sep))
    fmt = "{0}_{1}_ccdc_{2}"
    return merlin.functions.cqlstr(fmt.format(ard, aux, ver)).strip().lower().lstrip('_')


def conf():
    return {'spark.driver.host':                          HOST,
            'spark.mesos.principal':                      MESOS_PRINCIPAL,
            'spark.mesos.secret':                         MESOS_SECRET,
            'spark.mesos.role':                           MESOS_ROLE,
            'spark.mesos.executor.docker.image':          IMAGE,
            'spark.mesos.executor.docker.forcePullImage': FORCE_PULL,
            'spark.mesos.task.labels':                    'lcmap-ccdc:{}'.format(os.environ['USER']),
            'spark.serializer':                           SERIALIZER,
            'spark.python.worker.memory':                 PYTHON_WORKER_MEMORY,
            'spark.executor.cores':                       '1',
            'spark.cores.max':                            CORES_MAX,
            'spark.executor.memory':                      EXECUTOR_MEMORY}


def context(name, conf=conf()):
    """ Create or return a Spark Context

    Args:
        name  (str): Name of the application
        conf (dict): Spark configuration

    Returns:
        A Spark context
    """

    return SparkContext(master=os.environ['MASTER'],
                        appName='lcmap-ccdc:{}'.format(os.environ['USER']),
                        conf=SparkConf().setAll(conf.items()))


# Must obtain a logger from log4j since the jvm is what is actually 
# doing all the logging under the covers for PySpark.
# Format and logging configuration is handled through log4j.properties.

def logger(context, name):
    """Get PySpark configured logger
    
    Args:
        context: SparkContext
        name (str): Name of the logger (category)

    Returns:
        Logger instance
    """

    # TODO: add ccdc version to name
    return context._jvm.org.apache.log4j.LogManager.getLogger(name)
