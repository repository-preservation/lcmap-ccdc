from pyspark import SparkConf, SparkContext


def sparkcon():
    return SparkContext("local", "TestContext")