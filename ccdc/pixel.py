from ccdc import cassandra
from pyspark.sql.types import ArrayType
from pyspark.sql.types import ByteType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """Cassandra table name"""
    return 'pixel'


def schema():
    """Schema for pixel dataframe"""
    return StructType([
        StructField('cx'    , IntegerType(), nullable=False),
        StructField('cy'    , IntegerType(), nullable=False),
        StructField('px'    , IntegerType(), nullable=False),
        StructField('py'    , IntegerType(), nullable=False),
        StructField('mask'  , ArrayType(ByteType()), nullable=True)])\


def dataframe(ctx, ccd):
    """Create pixel dataframe from ccd dataframe

    Args:
        ctx: spark context
        ccd: CCD dataframe

    Returns:
        dataframe conforming to pixel.py
    """
        
    return ccd.select(schema().fieldNames())


def read(ctx, ids):
    """Read pixels

        ctx: spark context
        ids: dataframe of (cx, cy)

    Returns:
        dataframe conforming to pixel.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['cx', 'cy'],
                    how='inner')


def write(ctx, df):
    """Write pixels

    Args:
        ctx: spark context
        df : dataframe conforming to pixel.schema()
    
    Returns:
        df
    """
    
    cassandra.write(ctx, df, table())
    return df
