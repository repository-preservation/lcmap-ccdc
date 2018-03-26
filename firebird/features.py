from cytoolz import first
from cytoolz import thread_last
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import udf


def join(dfs):
    """Join aux and ccd dataframes

    Args:
        dfs (dict): {'aux': aux dataframe, 'ccd': ccd dataframe}

    Returns:
        combined dataframe
    """
    return dfs['aux'].join(dfs['ccd'],
                           on=['chipx', 'chipy', 'x', 'y'],
                           how='inner')


def columns():
    """Return list of columns used for generating independent variable.  

    Order of values is significant.
    
    Returns:
        list of column names.
    """
    #########################################################################
    # WARNING!  Altering this list invalidates all persisted models
    # and classifications.
    #########################################################################
     
    return ['blmag',  'grmag',  'remag',  'nimag',  's1mag',  's2mag',  'thmag',
            'blrmse', 'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse',
            'blcoef', 'grcoef', 'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef',
            'blint',  'grint',  'reint',  'niint',  's1int',  's1int',  'thint',
            'dem',    'aspect', 'slope',  'mpw',    'posidex'])
            

@udf(returnType=VectorUDF())
def densify(*args, **kwargs):
    """Creates classification features

    Args:
        *args    - sequence of arguments to be densified into Vector
        **kwargs - ignored

    Returns:
        pyspark.ml.linalg.Vector
    """
    
    return Vectors.dense(list(map(lambda x: first(x) if type(x) in [tuple, set, list], args)))


def dependent(df):
    """Creates label column on dataframe"""
    return df.withColumnRenamed('trends', 'label')


def independent(df):
    """User defined function to create features for a dataframe"""
    # call UDF to create features column and return df with column
    return df.with_column('features', densify(*columns()))


def dataframe(aux, ccd):
    """Create a dataframe suitable for training and classification

    Args:
        aux: aux dataframe
        ccd: ccd dataframe

    Returns:
        dataframe with location, label and features
    """

    df = thread_last({'aux': aux, 'ccd': ccd},
                     join,
                     dependent,
                     independent})

    return df.select(['chipx', 'chipy', 'x', 'y', 'label', 'features'])
