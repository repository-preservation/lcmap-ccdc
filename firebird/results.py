from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

def missing(bounds, rdd):
    pass

def errors(rdd):
    pass

def count(rdd):
    pass

def dataframe(rdd, schema):
    pass

def success(chip_x, chip_y, x, y, algorithm, datestr, data):
    return {}
    
def failure(chip_x, chip_y, x, y, algorithm, datestr, data):
    return {'error': 1}


def schema(data_type):
    """Base dataframe schema for results

    Returns:
        pyspark.sql.types.StructType: Schema for result
    """

    fields = [StructField('chip_x', FloatType(), nullable=False),
              StructField('chip_y', FloatType(), nullable=False),
              StructField('x', FloatType(), nullable=False),
              StructField('y', FloatType(), nullable=False),
              StructField('datestr', StringType(), nullable=False),
              StructField('result', data_type, nullable=False),
              StructField('error', BooleanType(), nullable=False),
              StructField('jobconf', StringType(), nullable=False)]
    return StructType(fields)
