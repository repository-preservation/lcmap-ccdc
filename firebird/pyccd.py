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

import cassandra
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
    return StructType([StructField('chipx', IntegerType(), nullable=False),
                       StructField('chipy', IntegerType(), nullable=False),
                       StructField('x', IntegerType(), nullable=False),
                       StructField('y', IntegerType(), nullable=False),
                       StructField('mask', ArrayType(IntegerType()), nullable=False),
                       StructField('procedure', StringType(), nullable=False),
                       StructField('models', StringType(), nullable=False)])
           

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
    r = rdd.map(lambda r: (r[0], r[1], r[2], r[3], *unpack(r[4])))
    return sql.SparkSession(sc).createDataFrame(r, schema=schema())
    

def read(sc, tilex, tiley):
    """Reads a tile of change results from Cassandra

    Args:
        tilex (int): tile x coordinate
        tiley (int): tile y coordinate
        
    Returns:
        A spark dataframe conforming to pyccd.schema
    """
    pass


#def write(sc, dataframe):
#    """Writes a dataframe to persistent storage
#
#    Args:
#        sc: Spark Context
#        dataframe: Dataframe to persist
#
#    Returns:
#        dataframe
#    """
#    
#    return cassandra.write(sc=sc,
#                           dataframe=dataframe,
#                           options=cassandra.options(table=cqlstr(ccd.algorithm)))
    

def execute(sc, timeseries):
    """Run pyccd against a timeseries

    Args:
        sc: spark context
        timeseries: RDD of timeseries data

    Returns:
        RDD of pyccd results
    """
    
    ts = timeseries.cache()
    logger(context=sc, name=__name__).info('executing change detection')
    return ts.map(lambda t: (*t[0], ccd.detect(**inputs(t[1]))))
            

def udf():
    """Dataframe user defined function for pyccd"""
    pass
