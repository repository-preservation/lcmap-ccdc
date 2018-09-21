from ccdc import cassandra
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """ Cassandra segment table name """
    
    return 'segment'


def schema():
    return StructType([
        StructField('cx'    , IntegerType(), nullable=False),
        StructField('cy'    , IntegerType(), nullable=False),
        StructField('px'    , IntegerType(), nullable=False),
        StructField('py'    , IntegerType(), nullable=False),
        StructField('sday'  , StringType(), nullable=False),
        StructField('eday'  , StringType(), nullable=False),
        StructField('bday'  , StringType(), nullable=True),
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
        StructField('rfrawp', ArrayType(FloatType()), nullable=True)
    ])


def dataframe(ctx, ccd):
    """Create segment dataframe from ccd dataframe

    Args:
        ctx: spark context
        ccd: CCD dataframe

    Returns:
        dataframe conforming to segment.schema()
    """
        
    return ccd.select(schema().fieldNames())


def read(ctx, ids):
    """Read segments

    Args:
        ctx: spark context
        ids: dataframe of (cx, cy)

    Returns:
        dataframe conforming to segment.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['cx', 'cy'],
                    how='inner')


def write(ctx, df):
    """Write segments

    Args:
        ctx: spark context
        df : dataframe conforming to segment.schema()
    
    Returns:
        df
    """
    cassandra.write(ctx, df, table())
    return df


def join(segments, predictions):
    """Join segments dataframe with predictions dataframe

    Args:
        segments:    segments dataframe
        predictions: predictions dataframe

    Returns:
        dataframe
    """
    
    return segments.join(predictions,
                         on=['cx', 'cy', 'px', 'py', 'sday', 'eday'],
                         how='inner').drop(segments['rfrawp'])
