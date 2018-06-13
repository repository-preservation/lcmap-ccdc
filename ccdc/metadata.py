from ccdc import cassandra
from ccdc import logger
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

import datetime


def table():
    """Cassandra metadata table name"""
    
    return 'metadata'


def schema():
    """pyspark dataframe schema for metadata"""
    
    return StructType([
        StructField('ulx', IntegerType(), nullable=False),
        StructField('uly', IntegerType(), nullable=False),
        StructField('lrx', IntegerType(), nullable=True),
        StructField('lry', IntegerType(), nullable=True),
        StructField('h', IntegerType(), nullable=True),
        StructField('v', IntegerType(), nullable=True),
        StructField('acq', StringType(), nullable=True),
        StructField('detector', StringType(), nullable=True),
        StructField('dran', StringType(), nullable=True),
        StructField('segcnt', IntegerType(), nullable=True),
        StructField('classifier', StringType(), nullable=True),
        StructField('cran', StringType(), nullable=True),
        StructField('ardurl', StringType(), nullable=True),
        StructField('auxurl', StringType(), nullable=True)])


def detection(ulx, uly, lrx, lry, h, v, acquired, detector, ardurl, segcount):
    """create metadata for detectors

    Args:
        ulx:   upper left x
        uly:   upper left y
        lrx:   lower right x
        lry:   lower right y
        h:     horizontal id of tile
        v:     vertical id of tile
        acquired: ISO8601 date range for input data (str)
        detector: name and version of detector
        ardurl: URL used to supply input ARD data to detector
        segcount: number of segments written

    Returns:
        dict: Detector metadata
    """
    
    return {'ulx': ulx,
            'uly': uly,
            'lrx': lrx,
            'lry': lry,
            'h': h,
            'v': v,
            'acq': acquired,
            'detector': detector,
            'dran': datetime.datetime.now().isoformat(),
            'ardurl': ardurl,
            'segcnt': segcount,
            'classifier': None,
            'cran': None,
            'auxurl': None}


def classify(ulx, uly, classifier, auxurl):
    """create metadata for classifiers

    Args:
        ulx: x coordinate of tile
        uly: y coordinate of tile
        detector: name and version of classifier
        params: parameters supplied to classifier at runtime

    Returns:
        dict: Classifier metadata
    """

    return {'ulx': ulx,
            'uly': uly,
            'classifier': classifier,
            'cran': datetime.datetime.now().isoformat(),
            'auxurl': auxurl}


def dataframe(ctx, d):
    """Creates metadata dataframe from dictionary

    Args:
        ctx: spark context
        rdd: dictionary

    Returns:
        A spark dataframe conforming to schema()
    """

    logger(ctx, name=__name__).debug('creating metadata dataframe...')
    return SparkSession(ctx).createDataFrame([Row(**d)], schema=schema())
 
       
def read(ctx, ids):
    """Read metadata results

    Args:
        ctx: spark context
        ids: dataframe of (tilex, tiley)

    Returns:
        dataframe conforming to metadata.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['ulx', 'uly'],
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
                           on=['ulx', 'uly'],
                           how='inner')
