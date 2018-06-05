from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import merge
from cytoolz import second
from firebird import cassandra
from firebird import logger
from merlin.functions import cqlstr
from merlin.functions import denumpify
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import ArrayType
from pyspark.sql.types import ByteType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """Cassandra metadata table name"""
    
    return 'metadata'


def schema():
    return StructType([
        StructField('tilex' , IntegerType(), nullable=False),
        StructField('tiley' , IntegerType(), nullable=False),
        StructField('ddate' , DateType(),    nullable=False),
        StructField('cdate' , DateType(),    nullable=True),
        StructField('dtype' , StringType(),  nullable=False),
        StructField('ctype' , StringType(),  nullable=True),
        StructField('ardurl', IntegerType(), nullable=False),
        StructField('auxurl', IntegerType(), nullable=False)])


def dataframe(ctx, rdd):
    """Creates metadata dataframe from ard dataframe

    Args:
        ctx: spark contextd
        rdd: pyccd rdd

    Returns:
        A spark dataframe conforming to schema()
    """

    logger(ctx, name=__name__).debug('creating metadata dataframe...')
    return SparkSession(ctx).createDataFrame(rdd, schema())
 
       
def detect(timeseries):
    """Takes in a timeseries and returns a list of detections
     
    Args:
        timeseries (dict): ard timeseries

    Return:
        sequence of change detections
    """

    chipx, chipy, x, y = first(timeseries)

    return format(chipx=chipx,
                  chipy=chipy,
                  x=x,
                  y=y,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


#def rdd(ctx, timeseries):
#    """Run pyccd against a timeseries
#
#    Args:
#        ctx: spark context
#        timeseries: RDD of timeseries data
#
#    Returns:
#        RDD of pyccd results
#    """
#    
#    logger(context=ctx, name=__name__).info('executing change detection...')
#    return timeseries.flatMap(detect)


def read(ctx, ids):
    """Read metadata results

    Args:
        ctx: spark context
        ids: dataframe of (tilex, tiley)

    Returns:
        dataframe conforming to metadata.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['tilex', 'tiley'],
                    how='inner')


def write(ctx, df):
    """Write metadata results

    Args:
        ctx: spark context
        df : dataframe conforming to metadata.schema()
    
    Returns:
        df
    """
    cassandra.write(ctx, df, table())
    return df


def join(detections, classifications):
    """Join detections metadata dataframe with classifications metadata dataframe

    Args:
        detections:         detections dataframe
        classifications:    classifications dataframe

    Returns:
        dataframe
    """
    
    return detections.join(classifications,
                           on=['tilex', 'tiley'],
                           how='inner')

