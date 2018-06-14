from ccdc    import features
from ccdc    import timeseries
from .shared import acquired
from .shared import faux_dataframe
from .shared import features_columns
from .shared import features_dframe
from .shared import merge
from .shared import merged_schema
from .shared import mock_timeseries_rdd

import pyspark.sql.types

def test_join(spark_context, ids_rdd, merlin_ard_config, merlin_aux_config):
    ard_df = timeseries.ard(spark_context, ids_rdd, acquired, cfg=merlin_ard_config)
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired, cfg=merlin_aux_config)
    joined = features.join({'aux': aux_df, 'ccd': ard_df})

    assert set(joined.columns) == set(merged_schema)

def test_columns():
    assert set(features.columns()) == set(features_columns)

def test_dependent(sql_context):
    fauxDF = faux_dataframe(sql_context, ['firstName', 'trends'], 'iter')
    dependDF = features.dependent(fauxDF)
    assert set(['trends', 'firstName', 'label']) == set(dependDF.columns)

def test_independent(sql_context):
    fauxDF  = faux_dataframe(sql_context, features.columns())
    indepDF = features.independent(fauxDF)
    w_features = merge([features_columns, ['features']])
    assert set(w_features) == set(indepDF.columns)

def test_dataframe(spark_context, sql_context, ids_rdd, merlin_aux_config):
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired, cfg=merlin_aux_config)
    fauxDF = faux_dataframe(sql_context, features_dframe)
    framed = features.dataframe(aux_df, fauxDF)
    assert set(['chipx', 'chipy', 'pixelx', 'pixely', 'sday', 'eday', 'label', 'features']) == set(framed.columns)
