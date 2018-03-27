from pyspark.sql.functions import udf

from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType


@udf(returnType=BooleanType())
def as_bool(x):
    return bool(x)


@udf(returnType=FloatType())
def as_float(x):
    return float(x)


@udf(returnType=IntegerType())
def as_int(x):
    return int(x)


