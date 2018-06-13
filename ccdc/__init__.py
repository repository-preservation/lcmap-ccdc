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
INPUT_PARTITIONS                   = int(os.getenv('INPUT_PARTITIONS', 2))
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


def context(name):
    """ Create or return a Spark Context

    Args:
        name (str): Name of the application

    Returns:
        A Spark context
    """

    return SparkContext.getOrCreate(SparkConf().setAppName(name))
 

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
