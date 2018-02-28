from cytoolz import assoc
from cytoolz import second
from firebird import logger
from merlin.functions import cqlstr
from merlin.functions import serialize
from pyspark import sql
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

import ccd
import firebird

"""
def model_schema():

    spectra= StructType([StructField('magnitude', FloatType()),
                         StructField('rmse', FloatType()),
                         StructField('coefficients', ArrayType(FloatType())),
                         StructField('intercept', FloatType())])

    model = StructType([StructField('start_day', IntegerType()),
                        StructField('end_day', IntegerType()),
                        StructField('break_day', IntegerType()),
                        StructField('observation_count', IntegerType()),
                        StructField('change_probability', FloatType()),
                        StructField('curve_qa', IntegerType()),
                        StructField('blue', spectra),
                        StructField('green', spectra),
                        StructField('red', spectra),
                        StructField('nir', spectra),
                        StructField('swir1', spectra),
                        StructField('swir2', spectra),
                        StructField('thermal', spectra)])

    result = StructType([StructField('chipx', IntegerType(), nullable=False),
                         StructField('chipy', IntegerType(), nullable=False),
                         StructField('x', IntegerType(), nullable=False),
                         StructField('y', IntegerType(), nullable=False),
                         StructField('dates', ArrayType(IntegerType()), nullable=False),
                         StructField('mask', ArrayType(IntegerType()), nullable=False),
                         StructField('procedure', StringType(), nullable=False),
                         StructField('models', ArrayType(model), nullable=False)])

    return result
"""

def schema():
   
    result = StructType([StructField('chipx', IntegerType(), nullable=False),
                         StructField('chipy', IntegerType(), nullable=False),
                         StructField('x', IntegerType(), nullable=False),
                         StructField('y', IntegerType(), nullable=False),
                         StructField('dates', ArrayType(IntegerType()), nullable=False),
                         StructField('mask', ArrayType(IntegerType()), nullable=False),
                         StructField('procedure', StringType(), nullable=False),
                         StructField('models', StringType(), nullable=False)])
                         
    return result


def inputs(timeseries):
    """Reshape timeseries to match what ccd expects
    
    Args:
        timeseries (dict): timeseries input for ccd

    Returns:
        dict: reshaped input for ccd
    """
    
    return {'dates'   : timeseries.get('dates'),
            'blues'   : timeseries.get('blues'),
            'greens'  : timeseries.get('greens'),
            'reds'    : timeseries.get('reds'),
            'nirs'    : timeseries.get('nirs'),
            'swir1s'  : timeseries.get('swir1s'),
            'swir2s'  : timeseries.get('swir2s'),
            'thermals': timeseries.get('thermals'),
            'quality' : timeseries.get('qas')}


def unpack(result):
    """Unpacks pyccd results into a flat tuple.

    Args:
        result (dict): A Pyccd result

    Returns:
        tuple: (processing_mask, procedure, change_models)
    """
    
    return tuple([result.get('processing_mask'),
                  result.get('procedure'),
                  second(serialize(result.get('change_models')))])


def dataframe(sc, rdd):
    """Creates pyccd dataframe from pyccd rdd

    Args:
        sc: spark context
        rdd: rdd of pyccd results

    Returns:
        A spark dataframe conforming to pyccd.schema
    """

    logger(sc, name=__name__).info('converting to dataframe')
    r = rdd.map(lambda r: (r[0], r[1], r[2], r[3], r[4], *unpack(r[5])))
    return sql.SparkSession(sc).createDataFrame(r, schema=schema())
    

def read(tilex, tiley):
    """Reads a tile of change results from Cassandra

    Args:
        tilex (int): tile x coordinate
        tiley (int): tile y coordinate
        
    Returns:
        A spark dataframe conforming to pyccd.schema
    """
    pass


def write(sc, dataframe):
        
    options = {
        'table': cqlstr(ccd.algorithm),
        'keyspace': firebird.CASSANDRA_KEYSPACE,
        'spark.cassandra.auth.username': firebird.CASSANDRA_USER,
        'spark.cassandra.auth.password': firebird.CASSANDRA_PASS,
        'spark.cassandra.connection.compression': 'LZ4',
        'spark.cassandra.connection.host': firebird.CASSANDRA_HOST,
        'spark.cassandra.connection.port': firebird.CASSANDRA_PORT,
        'spark.cassandra.input.consistency.level': 'QUORUM',
        'spark.cassandra.output.consistency.level': 'QUORUM'
    }

    msg = assoc(options, 'spark.cassandra.auth.password', 'XXXXX')
    logger(sc, name=__name__).info('writing dataframe:{}'.format(msg))
    
    dataframe.write.format('org.apache.spark.sql.cassandra')\
                   .mode('append').options(**options).save()

    return dataframe


def execute(sc, timeseries):
    """Run pyccd against a timeseries

    Args:
        sc: spark context
        timeseries: RDD of timeseries data

    Returns:
        RDD of pyccd results
    """
    
    logger(context=sc, name=__name__).info('executing change detection')
    return timeseries.map(lambda t: (*t[0], t[1].get('dates'), ccd.detect(**inputs(t[1]))))


