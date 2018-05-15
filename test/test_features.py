from firebird import features
from firebird import timeseries

from pyspark.sql import Row

from .shared import acquired
from .shared import ard_schema
from .shared import aux_schema
from .shared import faux_dataframe
from .shared import features_columns
from .shared import merge_lists
from .shared import merged_schema
from .shared import mock_timeseries_rdd

import pyspark.sql.types

def test_join(spark_context, monkeypatch, ids_rdd):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)

    ard_df = timeseries.ard(spark_context, ids_rdd, acquired)
    aux_df = timeseries.aux(spark_context, ids_rdd, acquired)
    joined = features.join({'aux': aux_df, 'ccd': ard_df})

    assert set(joined.columns) == set(merged_schema)

def test_columns():
    assert features.columns() == features_columns

def test_dependent(sql_context):
    fauxDF = faux_dataframe(sql_context, ['firstName', 'trends'], 'iter')
    dependDF = features.dependent(fauxDF)
    assert 'label' in dependDF.columns

def test_independent(sql_context):
    fauxDF  = faux_dataframe(sql_context, features.columns())
    indepDF = features.independent(fauxDF)
    assert 'features' in indepDF.columns

def test_dataframe(monkeypatch, spark_context, sql_context, ids_rdd):
    monkeypatch.setattr(timeseries, 'rdd', mock_timeseries_rdd)

    aux_df = timeseries.aux(spark_context, ids_rdd, acquired)
    # why do i need to strip these columns?
    fcl = [i for i in features.columns() if i not in ['dem', 'aspect', 'slope', 'mpw', 'posidex']]
    # and add these columns?
    fauxDF  = faux_dataframe(sql_context, merge_lists([fcl, ard_schema, ['sday', 'eday']]))
    framed = features.dataframe(aux_df, fauxDF)
    assert 'features' in framed.columns
