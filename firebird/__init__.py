import logging
import merlin
import os
import socket

# All of these are evaluated at import time!!!  That means the env vars need
# to be set before the firebird module is imported.  
HOST               = socket.gethostbyname(socket.getfqdn())
ARD_CHIPMUNK       = os.getenv('ARD_CHIPMUNK', 'http://localhost:5678')
AUX_CHIPMUNK       = os.getenv('AUX_CHIPMUNK', 'http://localhost:5678')
CASSANDRA_URLS     = os.getenv('CASSANDRA_URLS', HOST)
CASSANDRA_USER     = os.getenv('CASSANDRA_USER', 'cassandra')
CASSANDRA_PASS     = os.getenv('CASSANDRA_PASS', 'cassandra')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'firebird_local')
INPUT_PARTITIONS   = int(os.getenv('INPUT_PARTITIONS', 1))
PRODUCT_PARTITIONS = int(os.getenv('PRODUCT_PARTITIONS', 1))
LOG_LEVEL          = os.getenv('LOG_LEVEL', 'WARN')
ARD_CFG            = merlin.cfg.get(profile='chipmunk-ard', env={'CHIPMUNK_URL': ARD_CHIPMUNK}) 
AUX_CFG            = merlin.cfg.get(profile='chipmunk-aux', env={'CHIPMUNK_URL': AUX_CHIPMUNK}) 


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

