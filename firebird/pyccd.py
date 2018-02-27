from firebird import logger
from pyspark import sql
from pyspark.sql.functions import col
from pyspark.sql.functions import size
from pyspark.sql.types import ArrayType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

import ccd


def schema():
   
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

    ccd = StructType([StructField('algorithm', StringType()),
                      StructField('processing_mask', ArrayType(IntegerType())),
                      StructField('procedure', StringType()),
                      StructField('change_models', ArrayType(model))])

    result = StructType([StructField('chipx', IntegerType(), nullable=False),
                         StructField('chipy', IntegerType(), nullable=False),
                         StructField('x', IntegerType(), nullable=False),
                         StructField('y', IntegerType(), nullable=False),
                         StructField('acquired', StringType(), nullable=False),
                         StructField('algorithm', StringType(), nullable=False),
                         StructField('processing_mask', ArrayType(IntegerType()), nullable=False),
                         StructField('procedure', StringType(), nullable=False),
                         StructField('change_models', ArrayType(model), nullable=False)])
                         
    return result


# Probably not necessary as the shape already matches this coming in
def inputs(timeseries):
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
        tuple: (algorithm, processing_mask, procedure, change_models)
    """
    
    return tuple([result.get('algorithm'), result.get('processing_mask'), result.get('procedure'), result.get('change_models')])


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
    

def read(tilex, tiley, acquired, algorithm):
    """Reads a tile of change results from Cassandra

    Args:
        tilex (int): tile x coordinate
        tiley (int): tile y coordinate
        acquired (str): ISO8601 date range used to generate change results
        algorithm (str): Algorithm version used to generate change results

    Returns:
        A spark dataframe conforming to pyccd.schema
    """
    pass


def write(sc, dataframe):
    
    logger(sc, name=__name__).info('dataframe sample:{}'.format(dataframe.where(size(col('change_models')) > 0).first()))
    return None


def execute(sc, timeseries):
    """Run pyccd against a timeseries

    Args:
        sc: spark context
        timeseries: RDD of timeseries data

    Returns:
        RDD of pyccd results
    """
    
    logger(context=sc, name=__name__).info('executing change detection')
    return timeseries.map(lambda t: (*t[0], ccd.detect(**inputs(t[1]))))

    #log.info("ccd detect:{}".format(rdd.count()))
    #log.info('mapped inputs:{}'.format(timeseries.map(inputs).first()))
