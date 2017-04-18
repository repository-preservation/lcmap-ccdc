import os

AARDVARK_HOST = os.getenv('AARDVARK_HOST', 'localhost')
AARDVARK_PORT = os.getenv('AARDVARK_PORT', '5678')
AARDVARK_CHIP_SPECS = os.getenv('AARDVARK_CHIP_SPECS', '/landsat/chip-specs')

CASSANDRA_CONTACT_POINTS = os.getenv('CASSANDRA_CONTACT_POINTS', '0.0.0.0')
CASSANDRA_USER = os.getenv('CASSANDRA_USER')
CASSANDRA_PASS = os.getenv('CASSANDRA_PASS')
CASSANDRA_KEYSPACE       = os.getenv('CASSANDRA_KEYSPACE', 'lcmap_changes_local')

LOG_LEVEL = ''

SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://localhost:7077')

LCMAP_PRODUCT_DICT = {
    "CCD": "Continuous Change Detection",
    "LOS": "Length of Segment",
    "QA" : "Quality Assurance",
    "MOC": "Magnitude of Change",
    "TSC": "Time Since Change",
    "LTC": "Location and Timing of Change"
}


def minbox(points):
    """ Returns the minimal bounding box necessary to contain points """
    pass
