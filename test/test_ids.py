from ccdc import ids

import json
import pyspark.rdd
import pyspark.sql.types
import pytest

def test_schema():
    ids_schema = ids.schema()
    assert type(ids_schema) == pyspark.sql.types.StructType
    assert json.loads(ids_schema.json()) == {'fields': [{'metadata': {}, 'name': 'chipx', 'nullable': False, 'type': 'integer'}, 
                                                        {'metadata': {}, 'name': 'chipy', 'nullable': False, 'type': 'integer'}], 'type': 'struct'}

def test_rdd(spark_context):
    rdd = ids.rdd(spark_context, ((-100, 100), (-200, 200)))
    assert type(rdd) == pyspark.rdd.RDD

def test_dataframe(spark_context):
    rdd = ids.rdd(spark_context, ((-100, 100), (-200, 200)))
    df = ids.dataframe(spark_context, rdd)
    assert type(df) == pyspark.sql.dataframe.DataFrame
    assert df.schema == ids.schema()
