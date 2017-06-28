from firebird import chip
from firebird import chip_specs
from firebird import functions as f
from firebird import transforms
from firebird import validation
from functools import partial
import firebird as fb


def broadcast(context, sparkcontext):
    """Sets read-only values on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    """
    return {k: sparkcontext.broadcast(value=v) for k,v in context.items()}


def init(acquired, chip_ids, products, product_dates, sparkcontext,
         chips_fn=chips.get,
         specs_fn=chip_specs.get,
         clip_box=None,
         initial_partitions=fb.INITIAL_PARTITION_COUNT,
         product_partitions=fb.PRODUCT_PARTITION_COUNT):

    """Constructs product graph and prepares Spark for execution
    :param acquired: Date values for selecting input products.
                     ISO format, joined with '/': <start date>/<end date>.
    :param chip_ids: Sequence of chip_ids (x, y)
    :param clip_box: Clip outputs that only fit within the supplied box.  If
                     None, no clipping is performed and full chips are produced
    :param products: A sequence of product names to deliver
    :param product_dates:  A sequence of iso format product dates to deliver
    :param chips_fn: func to return chips: chips(url, x, y, acquired, ubids)
    :param specs_fn: func to return specs: chip_specs(query)
    :param initial_partitions: Number of partitions for initial query
    :param product_partitions: Number of partitions for product generation
    :param sparkcontext: A SparkContext
    :return: True
    """
    # If clip_box is supplied we don't process locations that dont fit within
    # the requested bounds.  This allows us to run just a handful of pixels
    # for evaluation rather than an entire chip.
    #
    # Higher level functions that call driver.init() should handle converting
    # requests for bounding boxes or polygons (which are then converted to
    # a minbox) to a set of chip ids.
    #
    # The save functionality will allow us to save to iwds.  There will also be
    # an evaluate function which will save results to local files instead.
    # This should help speed up evaluation of algorithm changes.
    #
    # In order to implement training however, init will need to accept
    # chipids only and the caller will have to make calls to chip.ids(bbox)
    # to determine which ones to run.  The bbox clipping will still
    # be enabled however for test, eval, or just sanity.
    #
    # To run an entire ARD tile, one only need request a bbox with it's
    # bounds in a single request rather than making 2500 requests with the
    # current lcmap-changes and lcmap-change-worker.  We'll have to be
    # careful of course not to launch a DOS attack on aardvark when doing this
    # by controlling the # of chipid partitions.
    #
    # Likewise when saving results, we will have to control the # of
    # partitions and thus control the parallelism we are throwing at Cassandra
    # TODO: Probably need another partition control flag for saving.

    # raises appropriate exceptions on error
    validation.validate(acquired=acquired,
                        products=products,
                        product_dates=product_dates,
                        clip_box=clip_box,
                        chips_fn=chips_fn,
                        specs_fn=specs_fn)

    try:
        # retrieve a chip spec so we can generate chip ids
        queries = chip_spec_queries(fb.SPECS_URL)
        spec = specs_fn(queries['blues'])[0]

        # put the values we need on the cluster to make them available
        # to distributed functions.  They're in a dictionary to help
        # differentiate variables that have not been broadcast and to make
        # a testable function out of it
        # Broadcast variables are read-only on every node and must fit
        # in memory.
        # everything from here down is an RDD/broadcast variable/cluster op.
        # Don't mix up driver memory locations and cluster memory locations
        jobconf = broadcast(
                      context={'acquired': acquired,
                               'clip_box': clip_box,
                               'chip_ids': chip_ids,
                               'chips_fn': chips_fn,
                               'chip_spec_queries': queries,
                               'chips_url': fb.CHIPS_URL,
                               'clip_box': clip_box,
                               'initial_partitions': initial_partitions,
                               'products': products,
                               'product_dates': product_dates,
                               'product_partitions': product_partitions,
                               # should be able to pull this from the
                               # specs_fn and specs_url but this lets us
                               # do it once without beating aardvark up.
                               'reference_spec': spec,
                               'specs_url': fb.SPECS_URL,
                               'specs_fn': specs_fn},
                      sparkcontext=sparkcontext)

        fb.logger.info('Initializing product graph:{}'
                       .format({k: v.value for k, v in jobconf.items()}))

        graph = transforms.products(jobconf, sparkcontext)

        fb.logger.info('Product graph created:{}'
                       .format(graph.keys()))

        # product call graphs are created but not realized.  Do something with
        # whichever one you want in order to cause the computation to occur
        # (example: if curveqa is requested, save it and it will compute)
        # TODO: how am i going to get a cassandra connection from each
        # without creating 10,000 connections?
        return {'products': graph, 'jobconf': jobconf}

    except Exception as e:
        fb.logger.info("Exception generating firebird products: {}".format(e))
        raise e


def train():
    pass


def classify():
    pass


def evaluate(acquired, bounds, clip, products, product_dates, directory):
    pass


def save(acquired, bounds, clip, products, product_dates):

    def write(table, mode, rdd):
        struct = [['chip_x', 'chip_y', 'x', 'y', 'algorithm', 'datestr'],
                  'results', 'errors']
        df = ss.createDataFrame(rdd, struct)
        df.write.options(table=table, keyspace=fb.CASSANDRA_KEYSPACE).mode(mode).save()

    spec = chip_specs.get(fb.chip_spec_queries(fb.CHIPS_URL)['blues'])[0]
    ids  = chips.bounds_to_ids(bounds, spec)
    job  = init(acquired, ids, products, product_dates, sparkcontext)
    return [write(t, 'append', job[p]) for p in products]


def count(bounds, product):
    pass


def missing(bounds, product):
    pass


def errors(bounds, product):
    pass
