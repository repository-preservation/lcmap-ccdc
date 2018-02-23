import firebird
import merlin


def execute(sc, chips, acquired):
    """Create timeseries from a collection of chip ids and time range"""
    
    rdd = sc.parallelize(chips, firebird.INPUT_PARTITIONS)
    return rdd.map(merlin.create).repartition(firebird.PRODUCT_PARTITIONS).setname('timeseries.execute')


