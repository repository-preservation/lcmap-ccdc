from cytoolz import thread_last
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf

"""
        StructField('blmag' , FloatType(), nullable=True),
        StructField('grmag' , FloatType(), nullable=True),
        StructField('remag' , FloatType(), nullable=True),
        StructField('nimag' , FloatType(), nullable=True),
        StructField('s1mag' , FloatType(), nullable=True),
        StructField('s2mag' , FloatType(), nullable=True),
        StructField('thmag' , FloatType(), nullable=True),
        StructField('blrmse', FloatType(), nullable=True),
        StructField('grrmse', FloatType(), nullable=True),
        StructField('rermse', FloatType(), nullable=True),
        StructField('nirmse', FloatType(), nullable=True),
        StructField('s1rmse', FloatType(), nullable=True),
        StructField('s2rmse', FloatType(), nullable=True),
        StructField('thrmse', FloatType(), nullable=True),
        StructField('blcoef', ArrayType(FloatType()), nullable=True),
        StructField('grcoef', ArrayType(FloatType()), nullable=True),
        StructField('recoef', ArrayType(FloatType()), nullable=True),
        StructField('nicoef', ArrayType(FloatType()), nullable=True),
        StructField('s1coef', ArrayType(FloatType()), nullable=True),
        StructField('s2coef', ArrayType(FloatType()), nullable=True),
        StructField('thcoef', ArrayType(FloatType()), nullable=True),
        StructField('blint' , FloatType(), nullable=True),
        StructField('reint' , FloatType(), nullable=True),
        StructField('niint' , FloatType(), nullable=True),
        StructField('s1int' , FloatType(), nullable=True),
        StructField('s2int' , FloatType(), nullable=True),
        StructField('thint' , FloatType(), nullable=True),
"""

#def features(trends, mag, coefs, rmse, intercept, dem, aspect, posidex, slope, mpw):
#    """Dataframe user defined function to create training features"""
#    pass


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


def label(df):
    """Creates label column on dataframe"""
    return df.rename('trends', 'label')


def udf(df):
    """Creates classification features"""

    # This needs to be a UDF
    return (df.all_the_things)


def features(df):
    """User defined function to create features for a dataframe"""
    # call UDF to create features column and return df with column
    return independent(df)


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
                     label,
                     features})

    return df.select(['chipx', 'chipy', 'x', 'y', 'label', 'features'])
