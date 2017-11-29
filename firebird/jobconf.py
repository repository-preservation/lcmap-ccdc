def schema():
    pass


def initialize(acquired, bounds, products, product_dates, refspec,
               chips_fn=chips.get,
               specs_fn=chip_specs.get,
               clip_box=None,
               initial_partitions=fb.INITIAL_PARTITION_COUNT,
               product_partitions=fb.PRODUCT_PARTITION_COUNT()):
    return {}


def serialize():
    pass


def write():
    pass


def read(jobconf_id):
    pass


def execute(context):
    """Sets read-only values on the cluster to make them available to cluster
    operations.

    Args:
        context (dict): key:values to broadcast to the cluster

    Returns:
        dict: cluster references for each key: value pair
    """

    return {k: spark_context.broadcast(value=v) for k, v in context.items()}


def initialize(ccd=None, seglength=None, changemag=None, lastchange=None, train=(x,y)):
    pass

# come out and say that you must have run the previous step before
# running the next step?

# timeseries[1990-01-01/2000-01-01]
# ccd[1990-01-01/2000-01-01]
# train[x,y, 1990-01-01/2000-01-01]
# classify[]

# seglength[1990,2000,2010]
# changemag[1990,2000,2010]
# lastchange[1990,2000,2010]
