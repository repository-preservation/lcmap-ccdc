from cytoolz import first
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import ArrayType
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


@udf(returnType=VectorUDT())
def densify(*args, **kwargs):
    """Create classification features

    Args:
        *args    : sequence of arguments to be densified into Vector
        **kwargs : ignored

    Returns:
        pyspark.ml.linalg.Vector
    """

    fn = lambda x: first(x) if type(x) in [tuple, set, list] else x
    
    return Vectors.dense(list(map(fn, args)))


@udf(returnType=ArrayType(FloatType()))
def dedensify(vector):
    """Converts vector to Python list.

    Args:
        vector: pyspark.ml.linalg.Vector

    Returns:
        list()
    """
    
    return list(vector)
