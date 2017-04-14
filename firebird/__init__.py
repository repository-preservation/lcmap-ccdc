CASSANDRA_CONTACT_POINTS = os.getenv('CASSANDRA_CONTACT_POINTS', '0.0.0.0')
CASSANDRA_USER = os.getenv('CASSANDRA_USER')
CASSANDRA_PASS   = os.getenv('CASSANDRA_PASS')

LOG_LEVEL = ''

SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://localhost:7077')

AARDVARK_HOST = os.getenv('AARDVARK_HOST', 'localhost')
AARDVARK_PORT = os.getenv('AARDVARK_PORT', '5678')
AARDVARK_TILESPECS = os.getenv('AARDVARK_TILESPECS', '/landsat/tile-specs')

def minbox(points):
    """ Returns the minimal bounding box necessary to contain points """
    pass
