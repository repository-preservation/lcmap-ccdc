from cytoolz import assoc
from cytoolz import first
from cytoolz import second
from firebird import logger
from functools import partial
from pyspark import sql
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

import cassandra
import firebird
import merlin


def ids(sc, chips):
    """Loads chip ids into Spark
    
    Args:
        sc: Spark context
        chips (sequence): ((x1,y1),(x2,y2),(x3,y3)...))

    Returns:
        RDD of chip ids
    """
    
    logger(context=sc, name=__name__).info('loading chip ids')
    return sc.parallelize(chips, firebird.INPUT_PARTITIONS)


def schema():
     return StructType([StructField('chipx', IntegerType(), nullable=False),
                        StructField('chipy', IntegerType(), nullable=False),
                        StructField('dates', ArrayType(IntegerType()), nullable=False)])


def dataframe(sc, rdd):
    logger(sc, name=__name__).info('converting to dataframe')
    r = rdd.map(lambda r: (r[0][0], r[0][1], r[1].get('dates')))
    return sql.SparkSession(sc).createDataFrame(r, schema=schema())


def write(sc, dataframe):
    """Write a timeseries dataframe to persistent storage

    Args:
        sc: Spark Context
        dataframe: Dataframe to persist
    """

    return cassandra.write(sc=sc,
                           dataframe=dataframe,
                           options=cassandra.options(table='timeseries'))


def execute(sc, ids, acquired, cfg):
    """Create timeseries from a collection of chip ids and time range

    Args:
        sc: Spark context
        ids (rdd): RDD of chip ids
        acquired (str): ISO8601 date range: 1980-01-01/2017-01-01 
        cfg: A Merlin configuration

    Returns:
        RDD of time series: ((chipx, chipy, x, y, dates), {data}) 

    Example:
    >>> execute(sc, [(0,0)], '1980-01-01/2017-01-01', {a_merlin_cfg})
    >>> ((0, 0, 0, 0, '1980-01-01/2017-01-01'), 
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

    logger(context=sc, name=__name__).info('creating time series')
    fn = partial(merlin.create, acquired=acquired, cfg=cfg)
    return ids.map(lambda xy: fn(x=first(xy), y=second(xy)))\
              .flatMap(lambda x: x)\
              .map(lambda x: ((int(x[0][0]), int(x[0][1]), int(x[0][2]), int(x[0][3])), x[1]))\
              .repartition(firebird.PRODUCT_PARTITIONS)\
              .setName(__name__)



