from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import firebird


def schema():
    """Schema for ids dataframe"""
    
    return StructType([StructField('chipx', IntegerType(), nullable=False),
                       StructField('chipy', IntegerType(), nullable=False)])

def rdd(ctx, cids):
    """Creates rdd of chip ids
    
    Args:
        ctx             : spark context
        cids  (sequence): ((x1,y1),(x2,y2),(x3,y3)...))

    Returns:
        RDD of chip ids
    """

    return ctx.parallelize(cids, firebird.INPUT_PARTITIONS)
 
    
def dataframe(ctx, rdd):
    """Creates dataframe of (chipx, chipy)
 
    Args:
        rdd: ids.rdd

    Returns:
        dataframe conforming to ids.schema()
    """

    return SparkSession(ctx).createDataFrame(rdd, schema())
