from pyspark import SparkConf
from pyspark import SparkContext
import logging
import merlin
import os
import socket

# All of these are evaluated at import time!!!  That means the env vars need
# to be set before the firebird module is imported.  
HOST               = socket.gethostbyname(socket.getfqdn())
ARD_CHIPMUNK       = os.getenv('ARD_CHIPMUNK', 'http://localhost:5656')
AUX_CHIPMUNK       = os.getenv('AUX_CHIPMUNK', 'http://localhost:5656')
CASSANDRA_HOST     = os.getenv('CASSANDRA_URLS', HOST)
CASSANDRA_PORT     = int(os.getenv('CASSANDRA_PORT', 9043))
CASSANDRA_USER     = os.getenv('CASSANDRA_USER', 'cassandra')
CASSANDRA_PASS     = os.getenv('CASSANDRA_PASS', 'cassandra')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'lcmap_local')
INPUT_PARTITIONS   = int(os.getenv('INPUT_PARTITIONS', 1))
PRODUCT_PARTITIONS = int(os.getenv('PRODUCT_PARTITIONS', 1))
ARD                = merlin.cfg.get(profile='chipmunk-ard', env={'CHIPMUNK_URL': ARD_CHIPMUNK}) 
AUX                = merlin.cfg.get(profile='chipmunk-aux', env={'CHIPMUNK_URL': AUX_CHIPMUNK}) 


def context(name):
    """ Create or return a Spark Context

    Args:
        name (str): Name of the application

    Returns:
        A Spark context
    """

    return SparkContext.getOrCreate(conf=SparkConf().setAppName(name))
 

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
    
    return context._jvm.org.apache.log4j.LogManager.getLogger(name)

