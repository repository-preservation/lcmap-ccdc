from cytoolz import first
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import VectorUDT

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


 
