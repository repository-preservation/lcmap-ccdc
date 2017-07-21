from firebird import chips
from firebird import chip_specs
from firebird import functions as f
from firebird import transforms
from firebird import validation
from pyspark import sql
import firebird as fb
import logging
import pyspark

logger = logging.getLogger(__name__)


def broadcast(context, sparkcontext):
    """Sets read-only values on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    """
    return {k: sparkcontext.broadcast(value=v) for k, v in context.items()}


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
    :return: Tuple of the job graph, jobconf
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

    print('initing stuff and things...')

    # raises appropriate exceptions on error
    validation.validate(acquired=acquired,
                        products=products,
                        product_dates=product_dates,
                        clip_box=clip_box,
                        chips_fn=chips_fn,
                        specs_fn=specs_fn)

    try:
        # retrieve a chip spec so we can generate chip ids
        queries = fb.chip_spec_queries(fb.SPECS_URL)
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

        logger.info('Initializing job graph ...')
        logger.debug({k: v.value for k, v in jobconf.items()})

        job = transforms.products(jobconf, sparkcontext)

        logger.info('Job graph created ...')

        # product call graphs are created but not realized.  Do something with
        # whichever one you want in order to cause the computation to occur
        # (example: if curveqa is requested, save it and it will compute)
        return job, jobconf

    except Exception as e:
        logger.error("Exception generating firebird products: {}".format(e))
        raise e


def train():
    pass


def classify():
    pass


def write(table, dataframe, mode='append'):
    """Write a dataframe to Cassandra
    :param table: table name
    :param mode: append, overwrite, error, ignore
    :param dataframe: A Spark DataFrame
    """
    # https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md
    options = {
        'table': table,
        'keyspace': fb.CASSANDRA_KEYSPACE,
        'spark.cassandra.auth.username': fb.CASSANDRA_USER,
        'spark.cassandra.auth.password': fb.CASSANDRA_PASS,
        'spark.cassandra.connection.compression': 'LZ4',
        'spark.cassandra.connection.host': fb.CASSANDRA_CONTACT_POINTS,
        'spark.cassandra.input.consistency.level': 'QUORUM',
        'spark.cassandra.output.consistency.level': 'QUORUM'
    }
    return dataframe.write.format('org.apache.spark.sql.cassandra')\
                                  .mode(mode).options(**options).save()


def save(acquired, bounds, products, product_dates, clip=False,
         specs_fn=chip_specs.get, chips_fn=chips.get,
         sparkcontext_fn=pyspark.SparkContext):
    """Saves requested products to iwds
    :param acquired: / separated datestrings in iso8601 format.  Used to
                     determine the daterange of input data.
    :param bounds: sequence of points ((x1, y1), (x2, y2), ...).  Bounds are
                   minboxed and then corresponding chip ids are determined from
                   the result.
    :param products: sequence of products to save
    :param product_dates: sequence of product dates to produce and save
    :param clip: True, False.  If True any points not falling within the minbox
                 of bounds are filtered out.
    :return: iterator of results from calls to write()
    """
    print('saving stuff and things...')

    ss = None
    try:
        ss = sql.SparkSession(sparkcontext_fn())

        spec = specs_fn(fb.chip_spec_queries(fb.CHIPS_URL)['blues'])[0]

        ids  = chips.bounds_to_ids(bounds, spec)

        job, jobconf = init(acquired=acquired,
                            chip_ids=ids,
                            products=products,
                            product_dates=product_dates,
                            specs_fn=specs_fn,
                            chips_fn=chips_fn,
                            sparkcontext=ss.sparkContext,
                            clip_box=f.minbox(bounds) if clip else None)

        # first, save the jobconf used to generate the products
        md5, cfg = f.serialize({k: f.represent(v.value)
                                for k, v in jobconf.items()})
        jdf = ss.createDataFrame([[md5, cfg]], ['id', 'config'])
        write(table='jobconf', dataframe=jdf)

        # save all the products that were requested.  Add the jobconf id
        # for cross referencing.  Flatten the datastructure so it can be
        # inserted.  DataFrame doesn't like nested sequences for
        # field descriptions.
        #rdd structure: [[['chip_x', 'chip_y'], 'x', 'y', 'algorithm', 'datestr'],
        #                  'results', 'errors']
        schema = ['chip_x', 'chip_y', 'x', 'y', 'datestr',
                  'result', 'error', 'jobconf']
        for p in products:
            df = ss.createDataFrame(
                job[p].map(lambda x: (float(x[0][0][0]), float(x[0][0][1]),
                                      float(x[0][1]), float(x[0][2]),
                                      str(x[0][4]),
                                      str(x[1]), str(x[2]), str(md5)))\
                                      .repartition(fb.STORAGE_PARTITION_COUNT),
                schema=schema)

            yield write(table=f.cqlstr(job[p].name()), dataframe=df)
    finally:
        if ss is not None:
            ss.stop()


def count(bounds, product):
    pass


def missing(bounds, product):
    pass


def errors(bounds, product):
    pass
