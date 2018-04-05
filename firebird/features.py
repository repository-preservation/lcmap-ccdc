from cytoolz import first
from cytoolz import thread_last
from udfs import densify

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
            'dem',    'aspect', 'slope',  'mpw',    'posidex']
            

def dependent(df):
    """Create dependent variable

    Args:
        df: dataframe with trends column
    
    Returns:
        dataframe with label column
    """
    
    return df.withColumn('label', df.trends[0])


def independent(df):
    """Create independent variable

    Args:
        df: dataframe with columns as specified in columns()

    Returns:
        dataframe with features column
    """
    
    return df.withColumn('features', densify(*columns()))


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
                     independent)
    
    return df.select(['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'label', 'features']) 
