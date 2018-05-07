from firebird import ids
from pyspark.sql import SparkSession

import pytest
import json
import pyspark.sql.types
import pyspark.rdd

@pytest.fixture()
def spark_session():
    return SparkSession(pyspark.SparkContext.getOrCreate())

def test_schema():
    ids_schema = ids.schema()
    assert type(ids_schema) == pyspark.sql.types.StructType
    assert json.loads(ids_schema.json()) == {'fields': [{'metadata': {}, 'name': 'chipx', 'nullable': False, 'type': 'integer'}, 
                                                        {'metadata': {}, 'name': 'chipy', 'nullable': False, 'type': 'integer'}], 'type': 'struct'}

def test_rdd(spark_session):
    sc = spark_session.sparkContext
    rdd = ids.rdd(sc, ((-100, 100), (-200, 200)))
    assert type(rdd) == pyspark.rdd.RDD

def test_dataframe(spark_session):
    sc = spark_session.sparkContext
    rdd = ids.rdd(sc, ((-100, 100), (-200, 200)))
    df = ids.dataframe(sc, rdd)
    assert type(df) == pyspark.sql.dataframe.DataFrame
    assert df.schema == ids.schema()
