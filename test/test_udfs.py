from ccdc import udfs

import pyspark.sql.column

def test_densify():
    # validity in question
    dfy = udfs.densify("foo", "bar")
    assert type(dfy) is pyspark.sql.column.Column
