import pytest

from pyspark import SparkContext
from pyspark.sql import SparkSession

@pytest.fixture()
def spark_session():
    return SparkSession(SparkContext.getOrCreate())

@pytest.fixture()
def spark_context():
    return SparkSession(SparkContext.getOrCreate()).sparkContext
