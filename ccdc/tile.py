from ccdc import cassandra
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def table():
    """Cassandra tile table name"""
    
    return 'tile'


def schema():
    """ Schema for tile dataframe 

    TODO:  Consider ArrayType of ByteType for model persistence if necessary

    """
    
    return StructType([
        StructField('x'    ,   IntegerType(), nullable=False),
        StructField('y'    ,   IntegerType(), nullable=False),
        StructField('name',    StringType(),  nullable=False),
        StructField('model',   StringType(),  nullable=False),
        StructField('updated', StringType(),    nullable=False)
    ])


def rdd():
    return NotImplemented    


def dataframe(ctx, x, y, name, model):
    """ Create tile dataframe 

    Args:
        ctx:   Spark context
        x:     tile x
        y:     tile y
        name:  model name
        model: trained model

    Returns:
        Dataframe matching tile.schema()
    """
    pass


def read(ctx, ids):
    """Read tile results

    Args:
        ctx: spark context
        ids: dataframe of (x, y)

    Returns:
        dataframe conforming to tile.schema()
    """
    
    return ids.join(cassandra.read(ctx, table()),
                    on=['x', 'y'],
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
