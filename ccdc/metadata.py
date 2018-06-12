from ccdc import cassandra
from ccdc import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import MapType
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
        StructField('tilex', IntegerType(), nullable=False),
        StructField('tiley', IntegerType(), nullable=False),
        StructField('detector', StringType(), nullable=True),
        StructField('detector_ran', DateType(), nullable=True),
        StructField('detector_params', MapType(StringType(), StringType()), nullable=True),
        StructField('classifier', StringType(), nullable=True),
        StructField('classifier_ran', DateType(), nullable=True),
        StructField('classifier_params', MapType(StringType(), StringType()), nullable=True),
        StructField('ardurl', IntegerType(), nullable=True),
        StructField('auxurl', IntegerType(), nullable=True)])


def detector(tilex, tiley, detector, params, ardurl, auxurl):
    """create metadata for detectors

    Args:
        tilex: x coordinate of tile
        tiley: y coordinate of tile
        detector: name and version of detector
        params: parameters supplied to detector at runtime
        ardurl: URL used to supply input ARD data to detector
        auxurl: URL used to supply input AUX data to detector

    Returns:
        dict: Detector metadata
    """
    
    return {'tilex': tilex,
            'tiley': tiley,
            'detector': detector,
            'detector_ran': datetime.datetime.now().isoformat(),
            'detector_params': params,
            'ardurl': ardurl,
            'auxurl': auxurl}


def classifier(tilex, tiley, classifier, params):
    """create metadata for classifiers

    Args:
        tilex: x coordinate of tile
        tiley: y coordinate of tile
        detector: name and version of classifier
        params: parameters supplied to classifier at runtime

    Returns:
        dict: Classifier metadata
    """

    return {'tilex': tilex,
            'tiley': tiley,
            'classifier': classifier,
            'classifier_ran': datetime.datetime.now().isoformat(),
            'classifier_params': params}


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

