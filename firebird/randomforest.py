from firebird  import logger
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorIndexer
from pyspark.ml import Pipeline

import features
import ids
import pyccd
import timeseries
import udfs


def write():
    pass


def read():
    pass


def pipeline(fdf):
    # build the pipeline with indexers and return fitted model
    lindex   = StringIndexer(inputCol='label', outputCol='label_index').fit(fdf) 
    findex   = VectorIndexer(inputCol='features', outputCol='feature_index', maxCategories=8).fit(fdf)
    rf       = RandomForestClassifier(labelCol='label_index', featuresCol='feature_index', numTrees=500)
    return Pipeline(stages=[lindex, findex, rf])


def train(ctx, cids, acquired):
    """Trains a random forest model for a set of chip ids

    Args:
        cids (sequence): sequence of chip ids [(x,y), (x1, y1), ...]
        acquired (str): ISO8601 date range       
               
    Returns:
        A trained model or None
    """
    
    name = 'random-forest-training'

    log  = logger(ctx, name)

    # wire everything up
    aux  = timeseries.aux(ctx=ctx,
                          cids=cids,
                          acquired=acquired)\
                     .filter('trends[0] NOT IN (1, 8)').persist()
    
    aid  = aux.select(aux.chipx, aux.chipy).distinct()

    ccd  = pyccd.read(ctx, aid).filter('sday >= 0 AND eday >= 0')

    fdf  = features.dataframe(aux, ccd).persist()

    if fdf.count() == 0:
        log.info('No features found to train model')
        return None
    else:
        log.debug('sample feature:{}'.format(fdf.first()))
        log.debug('feature row count:{}'.format(fdf.count()))
        log.debug('feature columns:{}'.format(fdf.columns))

    model = pipeline(fdf).fit(fdf)

    # manage memory
    aux.unpersist()
    fdf.unpersist()
    
    return model


def classify(model, dataframe):
    """Classifies a dataframe using a trained random forest model

    Args:
        model: trained RandomForestClassifier
        dataframe: A features dataframe

    Returns:
        dataframe of raw predictions
    """
    #return model.transform(dataframe)\
    #            .select(['chipx', 'chipy', 'x', 'y', 'rawPrediction'])\
    #            .withColumnRenamed('rawPrediction', 'rfrawp')

    #data_df=data_df.withColumn("Plays", df1["Plays"].cast(IntegerType()))

    # return model.transform(dataframe)\
    #             .withColumn('chipx', model('chipx').cast(IntegerType()))\
    #             .withColumn('chipy', model('chipy').cast(IntegerType()))\
    #             .withColumn('x'    , model('x').cast(IntegerType()))\
    #             .withColumn('y'    , model('y').cast(IntegerType()))\
    #             .withColumn('rfrawp', model('rawPrediction'))
             
    return model.transform(dataframe)
