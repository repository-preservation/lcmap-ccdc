from cytoolz import assoc
from cytoolz import first
from cytoolz import second
from firebird import context
from firebird import ids
from firebird import logger
from functools import partial
from merlin.functions import denumpify

from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

import cassandra
import firebird
import merlin
import numpy


def schema(name):
    """Return Dataframe schema for named timeseries
    
    Args:
        name (str): name of timeseries

    Returns:
        StructType: Dataframe schema
    """
    
    s = {'ard': StructType([StructField('chipx', IntegerType(), nullable=False),
                            StructField('chipy', IntegerType(), nullable=False),
                            StructField('x', IntegerType(), nullable=False),
                            StructField('y', IntegerType(), nullable=False),
                            StructField('dates', ArrayType(IntegerType(), False), nullable=False),
                            StructField('blues', ArrayType(IntegerType(), False), nullable=False),
                            StructField('greens', ArrayType(IntegerType(), False), nullable=False),
                            StructField('reds', ArrayType(IntegerType(), False), nullable=False),
                            StructField('nirs', ArrayType(IntegerType(), False), nullable=False),
                            StructField('swir1s', ArrayType(IntegerType(), False), nullable=False),
                            StructField('swir2s', ArrayType(IntegerType(), False), nullable=False),
                            StructField('thermals', ArrayType(IntegerType(), False), nullable=False),
                            StructField('qas', ArrayType(IntegerType(), False), nullable=False)]),
         'aux': StructType([StructField('chipx', IntegerType(), nullable=False),
                            StructField('chipy', IntegerType(), nullable=False),
                            StructField('x', IntegerType(), nullable=False),
                            StructField('y', IntegerType(), nullable=False),
                            StructField('dates', ArrayType(IntegerType(), False), nullable=False),
                            StructField('dem', ArrayType(FloatType(), False), nullable=True),
                            StructField('trends', ArrayType(IntegerType(), False), nullable=False),
                            StructField('aspect', ArrayType(IntegerType(), False), nullable=True),
                            StructField('posidex', ArrayType(FloatType(), False), nullable=True),
                            StructField('slope', ArrayType(FloatType(), False), nullable=True),
                            StructField('mpw', ArrayType(IntegerType(), False), nullable=True)])}

    return s.get(name) if name else s


def converter():
    """Returns a function to convert an RDD to Dataframe

    Returns:
        func: A Python function to convert an rdd to dataframe
    """

    return lambda x: {'chipx': int(x[0][0]),
                      'chipy': int(x[0][1]),
                      'x'    : int(x[0][2]),
                      'y'    : int(x[0][3]),
                      **denumpify(x[1])}


def dataframe(ctx, rdd):
    """Transforms an rdd to a dataframe

    Args:
        ctx: spark context
        rdd: rdd.  Name attribute must be set

    Returns:
        dataframe
    """

    logger(ctx, rdd.name()).info('converting {} to dataframe'.format(rdd.name()))
    session = SparkSession(ctx)
    return session.createDataFrame(data=rdd.map(converter()),
                                   schema=schema(rdd.name()))


def rdd(ctx, cids, acquired, cfg, name=__name__):
    """Create timeseries from a collection of chip ids and time range

    Args:
        ctx      : spark context
        cids (rdd): RDD of chip ids
        acquired (str): ISO8601 date range: 1980-01-01/2017-01-01 
        cfg: A Merlin configuration

    Returns:
        RDD of time series: ((chipx, chipy, x, y), {data}) 

    Example:
    >>> execute(sc, [(0,0)], '1980-01-01/2017-01-01', {a_merlin_cfg})
    >>> ((0, 0, 0, 0), 
         {'blues':    array([-9999, 295, -9999, 204, -9999, 238, -9999, -9999, 195, -9999, -9999, -9999], dtype=int16), 
          'qas':      array([1, 66, 1, 322, 1, 66, 1, 1, 66, 1, 1, 1], dtype=uint16), 
          'nirs':     array([-9999, 2329, -9999, 2379, -9999, 2115, -9999, -9999, 1629, -9999, -9999, -9999], dtype=int16), 
          'thermals': array([-9999, 3020, -9999, 2930, -9999, 2902, -9999, -9999, 2920, -9999, -9999, -9999], dtype=int16), 
          'swir2s':   array([-9999, 593, -9999, 593, -9999, 522, -9999, -9999, 375, -9999, -9999, -9999], dtype=int16), 
          'reds':     array([-9999, 413, -9999, 324, -9999, 315, -9999, -9999, 264, -9999, -9999, -9999], dtype=int16), 
          'swir1s':   array([-9999, 1322, -9999, 1205, -9999, 1100, -9999, -9999, 743, -9999, -9999, -9999], dtype=int16), 
          'greens':   array([-9999, 499, -9999, 422, -9999, 363, -9999, -9999, 334, -9999, -9999, -9999], dtype=int16), 
          'dates':    [734992, 734991, 734984, 734983, 734976, 734975, 734448, 734441, 734439, 727265, 726648, 726616]})
    """

    logger(ctx, name).info('creating time series')
    
    fn = partial(merlin.create, acquired=acquired, cfg=cfg)
    
    return cids\
        .flatMap(lambda xy: fn(x=first(xy), y=second(xy)))\
        .map(lambda x: ((int(x[0][0]), int(x[0][1]), int(x[0][2]), int(x[0][3])), x[1]))\
        .repartition(firebird.PRODUCT_PARTITIONS)\
        .setName(name)


def ard(ctx, cids, acquired):
    """Create an ard timeseries dataframe
    
    Args:
        ctx:  spark context
        cids: rdd off chip ids
        acquired (str): ISO8601 date range: 1980-01-01/2017-12-31

    Returns:
        ARD dataframe: ((chipx, chipy, x, y), {data}) 
    """
    
    return dataframe(ctx=ctx,
                     rdd=rdd(ctx=ctx,
                             cids=cids,
                             acquired=acquired,
                             cfg=firebird.ARD,
                             name='ard'))


def aux(ctx, cids, acquired):
    """Create an aux timeseries dataframe
    
    Args:
        ctx:  spark context
        cids: rdd off chip ids
        acquired (str): ISO8601 date range: 1980-01-01/2017-12-31

    Returns:
        Aux dataframe ((chipx, chipy, x, y), {data}) 
    """
    
    return dataframe(ctx=ctx,
                     rdd=rdd(ctx=ctx,
                             cids=cids,
                             acquired=acquired,
                             cfg=firebird.AUX,
                             name='aux'))
