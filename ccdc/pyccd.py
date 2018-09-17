from ccdc import cassandra
from ccdc import logger
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import merge
from cytoolz import second
from datetime import date
from merlin.functions import cqlstr
from merlin.functions import denumpify

from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import ArrayType
from pyspark.sql.types import ByteType
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

import ccd


def algorithm():
    """Returns The ccd algorithm and version"""

    return ccd.algorithm


def table():
    """Cassandra pyccd table name"""
    
    return 'data'


def schema():
    return StructType([
        StructField('cx'    , IntegerType(), nullable=False),
        StructField('cy'    , IntegerType(), nullable=False),
        StructField('px'    , IntegerType(), nullable=False),
        StructField('py'    , IntegerType(), nullable=False),
        StructField('sday'  , DateType(), nullable=False),
        StructField('eday'  , DateType(), nullable=False),
        StructField('bday'  , DateType(), nullable=True),
        StructField('chprob', FloatType(), nullable=True),
        StructField('curqa' , IntegerType(), nullable=True),
        StructField('blmag' , FloatType(), nullable=True),
        StructField('grmag' , FloatType(), nullable=True),
        StructField('remag' , FloatType(), nullable=True),
        StructField('nimag' , FloatType(), nullable=True),
        StructField('s1mag' , FloatType(), nullable=True),
        StructField('s2mag' , FloatType(), nullable=True),
        StructField('thmag' , FloatType(), nullable=True),
        StructField('blrmse', FloatType(), nullable=True),
        StructField('grrmse', FloatType(), nullable=True),
        StructField('rermse', FloatType(), nullable=True),
        StructField('nirmse', FloatType(), nullable=True),
        StructField('s1rmse', FloatType(), nullable=True),
        StructField('s2rmse', FloatType(), nullable=True),
        StructField('thrmse', FloatType(), nullable=True),
        StructField('blcoef', ArrayType(FloatType()), nullable=True),
        StructField('grcoef', ArrayType(FloatType()), nullable=True),
        StructField('recoef', ArrayType(FloatType()), nullable=True),
        StructField('nicoef', ArrayType(FloatType()), nullable=True),
        StructField('s1coef', ArrayType(FloatType()), nullable=True),
        StructField('s2coef', ArrayType(FloatType()), nullable=True),
        StructField('thcoef', ArrayType(FloatType()), nullable=True),
        StructField('blint' , FloatType(), nullable=True),
        StructField('grint' , FloatType(), nullable=True),
        StructField('reint' , FloatType(), nullable=True),
        StructField('niint' , FloatType(), nullable=True),
        StructField('s1int' , FloatType(), nullable=True),
        StructField('s2int' , FloatType(), nullable=True),
        StructField('thint' , FloatType(), nullable=True),
        StructField('dates' , ArrayType(DateType()), nullable=False),
        StructField('mask'  , ArrayType(ByteType()), nullable=True),
        StructField('rfrawp', ArrayType(FloatType()), nullable=True)
    ])


def dataframe(ctx, rdd):
    """Creates pyccd dataframe from ard dataframe

    Args:
        ctx: spark context
        rdd: pyccd rdd

    Returns:
        A spark dataframe conforming to schema()
    """

    logger(ctx, name=__name__).debug('creating pyccd dataframe...')
    return SparkSession(ctx).createDataFrame(rdd, schema())
 
   
def default(change_models):
    # if there are no change models, append an empty one to
    # signify that ccd was run for the point, setting start_day and end_day to day 1

    return [{'start_day': 1, 'end_day': 1, 'break_day': 1}] if not change_models else change_models

    
def format(cx, cy, px, py, dates, ccdresult):
       
    return [denumpify(
             {'cx'     : cx,
              'cy'     : cy,
              'px'     : px,
              'py'     : py,
              'sday'   : date.fromordinal(get('start_day', cm)),
              'eday'   : date.fromordinal(get('end_day', cm)),
              'bday'   : date.fromordinal(get('break_day', cm, None)),
              'chprob' : get('change_probability', cm, None),
              'curqa'  : get('curve_qa', cm, None),
              'blmag'  : get_in(['blue', 'magnitude'], cm, None),
              'grmag'  : get_in(['green', 'magnitude'], cm, None),
              'remag'  : get_in(['red', 'magnitude'], cm, None),
              'nimag'  : get_in(['nir', 'magnitude'], cm, None),
              's1mag'  : get_in(['swir1', 'magnitude'], cm, None),
              's2mag'  : get_in(['swir2', 'magnitude'], cm, None),
              'thmag'  : get_in(['thermal', 'magnitude'], cm, None),
              'blrmse' : get_in(['blue', 'rmse'], cm, None),
              'grrmse' : get_in(['green', 'rmse'], cm, None),
              'rermse' : get_in(['red', 'rmse'], cm, None),
              'nirmse' : get_in(['nir', 'rmse'], cm, None),
              's1rmse' : get_in(['swir1', 'rmse'], cm, None),
              's2rmse' : get_in(['swir2', 'rmse'], cm, None),
              'thrmse' : get_in(['thermal', 'rmse'], cm, None),
              'blcoef' : get_in(['blue', 'coefficients'], cm, None),
              'grcoef' : get_in(['green', 'coefficients'], cm, None),
              'recoef' : get_in(['red', 'coefficients'], cm, None),
              'nicoef' : get_in(['nir', 'coefficients'], cm, None),
              's1coef' : get_in(['swir1', 'coefficients'], cm, None),
              's2coef' : get_in(['swir2', 'coefficients'], cm, None),
              'thcoef' : get_in(['thermal', 'coefficients'], cm, None),
              'blint'  : get_in(['blue', 'intercept'], cm, None),
              'grint'  : get_in(['green', 'intercept'], cm, None),
              'reint'  : get_in(['red', 'intercept'], cm, None),
              'niint'  : get_in(['nir', 'intercept'], cm, None),
              's1int'  : get_in(['swir1', 'intercept'], cm, None),
              's2int'  : get_in(['swir2', 'intercept'], cm, None),
              'thint'  : get_in(['thermal', 'intercept'], cm, None),
              'dates'  : [date.fromordinal(o) for o in dates],
              'mask'   : get('processing_mask', ccdresult, None)})
             for cm in default(get('change_models', ccdresult, None))]


def detect(timeseries):
    """Takes in a timeseries and returns a list of detections
     
    Args:
        timeseries (dict): ard timeseries

    Return:
        sequence of change detections
    """

    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


def rdd(ctx, timeseries):
    """Run pyccd against a timeseries

    Args:
        ctx: spark context
        timeseries: RDD of timeseries data

    Returns:
        RDD of pyccd results
    """
    
    logger(context=ctx, name=__name__).info('executing change detection...')
    return timeseries.flatMap(detect)


#def read(ctx, ids):
#    """Read pyccd results
#
#    Args:
#        ctx: spark context
#        ids: dataframe of (chipx, chipy)
#
#    Returns:
#        dataframe conforming to pyccd.schema()
#    """
#    
#    return ids.join(cassandra.read(ctx, table()),
#                    on=['chipx', 'chipy'],
#                    how='inner')


#def write(ctx, df):
#    """Write pyccd results
#
#    Args:
#        ctx: spark context
#        df : dataframe conforming to pyccd.schema()
#    
#    Returns:
#        df
#    """
#    cassandra.write(ctx, df, table())
#    return df


#def join(ccd, predictions):
#    """Join ccd dataframe with predictions dataframe
#
#    Args:
#        ccd:         ccd dataframe
#        predictions: predictions dataframe
#
#    Returns:
#        dataframe
#    """
#    
#    return ccd.join(predictions,
#                    on=['chipx', 'chipy', 'pixelx', 'pixely', 'sday', 'eday'],
#                    how='inner').drop(ccd['rfrawp'])

