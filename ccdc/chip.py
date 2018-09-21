from ccdc import cassandra
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """Cassandra table name"""
    
    return 'chip'


def schema():
    """ Schema for chip dataframe """

    return StructType([
        StructField('cx'   , IntegerType(), nullable=False),
        StructField('cy'   , IntegerType(), nullable=False),
        StructField('dates', ArrayType(StringType()), nullable=False),      
    ])


def dataframe(ctx, ccd):
    """Create chip dataframe from ccd dataframe

    Args:
        ctx: spark context
        ccd: CCD dataframe

    Returns:
        dataframe conforming to chip.py
    """
    
    return ccd.select(schema().fieldNames())


def read(ctx, ids):
    """Read chip

        ctx: spark context
        ids: dataframe of (cx, cy)

    Returns:
        dataframe conforming to chip.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['cx', 'cy'],
                    how='inner')


def write(ctx, df):
    """Write chips

    Args:
        ctx: spark context
        df : dataframe conforming to chip.schema()
    
    Returns:
        df
    """

    cassandra.write(ctx, df, table())
    return df
