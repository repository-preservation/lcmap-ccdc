from merlin import chip_specs
from merlin import chips
from pyspark.sql import SparkSession
import logging
import os
import socket

# All of these are evaluated at import time!!!  That means the env vars need
# to be set before the firebird module is imported.  
HOST = socket.gethostbyname(socket.getfqdn())

SPECS_URL = os.getenv('FIREBIRD_SPECS_URL', 'http://localhost:5678/v1/landsat/chip-specs')
CHIPS_URL = os.getenv('FIREBIRD_CHIPS_URL', 'http://localhost:5678/v1/landsat/chips')

CASSANDRA_CONTACT_POINTS = os.getenv('FIREBIRD_CASSANDRA_CONTACT_POINTS', HOST)
CASSANDRA_USER = os.getenv('FIREBIRD_CASSANDRA_USER', 'cassandra')
CASSANDRA_PASS = os.getenv('FIREBIRD_CASSANDRA_PASS', 'cassandra')
CASSANDRA_KEYSPACE = os.getenv('FIREBIRD_CASSANDRA_KEYSPACE', 'lcmap_changes_local')

INITIAL_PARTITION_COUNT = int(os.getenv('FIREBIRD_INITIAL_PARTITION_COUNT', 1))
PRODUCT_PARTITION_COUNT = int(os.getenv('FIREBIRD_PRODUCT_PARTITION_COUNT', 1))
STORAGE_PARTITION_COUNT = int(os.getenv('FIREBIRD_STORAGE_PARTITION_COUNT', 1))

LOG_LEVEL = os.getenv('FIREBIRD_LOG_LEVEL', 'WARN')


def spark_session():
    """Return a spark session in a consistent way to the application
 
    Returns:
        pyspark.sql.SparkSession
    """
    
    return SparkSession.builder.getOrCreate()


# Must obtain a logger from log4j since the jvm is what is actually 
# doing all the logging under the covers for PySpark.
# Format and logging configuration is handled through log4j.properties.
def get_logger(sc, name):
    """Get PySpark configured logger
    
    Args:
        sc: SparkContext
        name (str): Name of the logger (category)

    Returns:
        Logger instance
    """

    return sc._jvm.org.apache.log4j.LogManager.getLogger(name)


def specs_fn(query):
    """Default specs function for firebird

    Args:
        query (dict): {"key1": "url query 1", "key2": "url query 2"}

    Returns:
        Function that accepts a query parameter and returns specs
    """

    return chip_specs.get


def chips_fn(url=CHIPS_URL):
    """Default chips function for firebird

    Args:
        url (str): URL to chips endpoint

    Returns:
        Partial function that accepts point, acquired and ubids and returns chips
    """
    
    #return partial(chips.get, url=url)
    return chips.get

    
def chip_spec_queries(url):
    """A map of pyccd spectra to chip-spec queries

    Args:
        url (str): full url (http://host:port/context) for chip-spec endpoint

    Returns:
        dict: spectra to chip spec queries

    Example:
        >>> chip_spec_queries('http://host/v1/landsat/chip-specs')
        {'reds':     'http://host/v1/landsat/chip-specs?q=tags:red AND sr',
         'greens':   'http://host/v1/landsat/chip-specs?q=tags:green AND sr'
         'blues':    'http://host/v1/landsat/chip-specs?q=tags:blue AND sr'
         'nirs':     'http://host/v1/landsat/chip-specs?q=tags:nir AND sr'
         'swir1s':   'http://host/v1/landsat/chip-specs?q=tags:swir1 AND sr'
         'swir2s':   'http://host/v1/landsat/chip-specs?q=tags:swir2 AND sr'
         'thermals': 'http://host/v1/landsat/chip-specs?q=tags:thermal AND ta'
         'quality':  'http://host/v1/landsat/chip-specs?q=tags:pixelqa'}
    """

    return {'reds':     ''.join([url, '?q=tags:red AND sr']),
            'greens':   ''.join([url, '?q=tags:green AND sr']),
            'blues':    ''.join([url, '?q=tags:blue AND sr']),
            'nirs':     ''.join([url, '?q=tags:nir AND sr']),
            'swir1s':   ''.join([url, '?q=tags:swir1 AND sr']),
            'swir2s':   ''.join([url, '?q=tags:swir2 AND sr']),
            'thermals': ''.join([url, '?q=tags:bt AND thermal AND NOT tirs2']),
            'quality':  ''.join([url, '?q=tags:pixelqa'])}
