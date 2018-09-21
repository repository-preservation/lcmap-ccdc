from ccdc import cassandra
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """Cassandra tile table name"""
    
    return 'tile'


def schema():
    """ Schema for tile dataframe """
    
    return StructType([
        StructField('tx',      IntegerType(), nullable=False),
        StructField('ty',      IntegerType(), nullable=False),
        StructField('name',    StringType(),  nullable=False),
        StructField('model',   StringType(),  nullable=False),
        StructField('updated', StringType(),  nullable=False)
    ])


def dataframe(ctx, tx, ty, name, model, updated):
    """ Create tile dataframe 

    Args:
        ctx:     Spark context
        tx:      tile x
        ty:      tile y
        name:    model name
        model:   trained model
        updated: iso format timestamp

    Returns:
        Dataframe matching tile.schema()
    """
    rows = [Row(tx=tx, ty=ty, name=name, model=model, updated=updated)]
    return SparkSession(ctx).createDataFrame(rows)


def read(ctx, ids):
    """Read tile results

    Args:
        ctx: spark context
        ids: dataframe of (x, y)

    Returns:
        dataframe conforming to tile.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['tx', 'ty'],
                    how='inner')


def write(ctx, df):
    """Write tile

    Args:
        ctx: spark context
        df:  dataframe conforming to tile.schema()

    Returns:
        df
    """
    cassandra.write(ctx, df, table())
    return df
