from cytoolz import first
from firebird import transforms
from merlin import chips
from merlin import chip_specs
from merlin import functions as f
from pyspark import sql
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import firebird as fb
import logging
import pyspark

logger = logging.getLogger(__name__)


def broadcast(context, spark_context):
    """Sets read-only values on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param spark_context: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    """
    return {k: spark_context.broadcast(value=v) for k, v in context.items()}


def init(acquired, chip_ids, products, product_dates, spark_context, refspec,
         chips_fn=chips.get,
         specs_fn=chip_specs.get,
         clip_box=None,
         initial_partitions=fb.INITIAL_PARTITION_COUNT,
         product_partitions=fb.PRODUCT_PARTITION_COUNT):

    """Constructs product graph and prepares Spark for execution

    Args:
        acquired (str): Date values for selecting input products.
                        ISO format, joined with '/': <start date>/<end date>.
        chip_ids (sequence): chip_ids (x, y)
        clip_box (dict): {'ulx':, 'uly':, 'lrx':, 'lry':,}
                         Clip outputs that only fit within the supplied box.
                         If None, no clipping is performed and full chips
                         are produced
        products (sequence): Products to produce
        product_dates (sequence):  iso8601 product dates
        spark_context: connection to a spark cluster
        chips_fn (func): func to return chips: chips(url, x, y, acquired, ubids)
        specs_fn (func): func to return specs: chip_specs(query)
        initial_partitions (int): Number of partitions for initial query
        product_partitions (int): Number of partitions for product generation
        spark_context: A SparkContext

    Returns:
        tuple: job, jobconf
    """

    # In order to implement training, init will need to accept
    # chipids only and the caller will have to make calls to chip.ids(bbox)
    # to determine which ones to run.  The bbox clipping will still
    # be enabled however for test, eval, or just sanity.

    queries = fb.chip_spec_queries(fb.SPECS_URL)

    try:
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
                               'reference_spec': refspec,
                               'specs_fn': specs_fn},
                      spark_context=spark_context)

        logger.info('Initializing job graph ...')
        logger.debug({k: v.value for k, v in jobconf.items()})

        job = transforms.products(jobconf, spark_context)
        logger.info('Job graph created ...')
        return job, jobconf
    except Exception as e:
        logger.error("Exception generating firebird products: {}".format(e))
        raise e


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

    dataframe.write.format('org.apache.spark.sql.cassandra')\
        .mode(mode).options(**options).save()

    return dataframe


def jobconf_schema():
    """Dataframe schema for jobconfs"""

    return StructType([StructField('id', StringType(), nullable=False),
                       StructField('config', StringType(), nullable=False)])


def result_schema():
    """Dataframe schema for results"""

    fields = [StructField('chip_x', FloatType(), nullable=False),
              StructField('chip_y', FloatType(), nullable=False),
              StructField('x', FloatType(), nullable=False),
              StructField('y', FloatType(), nullable=False),
              StructField('datestr', StringType(), nullable=False),
              StructField('result', StringType(), nullable=True),
              StructField('error', StringType(), nullable=True),
              StructField('jobconf', StringType(), nullable=False)]
    return StructType(fields)


def save(acquired, bounds, products, product_dates, spark_context, clip=False,
         specs_fn=chip_specs.get, chips_fn=chips.get):
    """Saves requested products to iwds

    Args:
        acquired (str): / separated datestrings in iso8601 format.  Used to
                        determine the daterange of input data.
        bounds (str): sequence of points ((x1, y1), (x2, y2), ...).  Bounds are
                      minboxed and then corresponding chip ids are determined
                      from the result.
        products (sequence): products to save
        product_dates (sequence): product dates to produce and save
        spark_context: a spark cluster connection
        clip (bool): If True any points not falling within the minbox of bounds
                     are filtered out.
                     
    Returns:
        generator: {product: dataframe}
    """

    ss = sql.SparkSession(spark_context)
    queries = fb.chip_spec_queries(fb.SPECS_URL)
    spec = first(specs_fn(queries[first(queries)]))
    coordinates  = chips.bounds_to_coordinates(tuple(bounds), spec)

    job, jobconf = init(acquired=acquired,
                        chip_ids=coordinates,
                        products=products,
                        product_dates=product_dates,
                        specs_fn=specs_fn,
                        refspec=spec,
                        chips_fn=chips_fn,
                        spark_context=spark_context,
                        clip_box=f.minbox(bounds) if clip else None)

    # first, save the jobconf used to generate the products
    md5, cfg = f.serialize({k: f.represent(v.value)
                            for k, v in jobconf.items()})

    write(table='jobconf',
          dataframe=ss.createDataFrame([[md5, cfg]], jobconf_schema()))

    for p in products:
        df = ss.createDataFrame(
            job[p].map(lambda x: (float(x[0][0]), float(x[0][1]),
                                  float(x[0][2]), float(x[0][3]),
                                  str(x[0][5]),
                                  str(x[1]), str(x[2]), str(md5)))\
                                  .repartition(fb.STORAGE_PARTITION_COUNT),
            schema=result_schema())

        yield {p: write(table=f.cqlstr(job[p].name()), dataframe=df)}


def count(dataframe):
    """Generates the success and error count for a dataframe"""

    return {'success': dataframe.where(dataframe['result'] != 'None').count(),
            'error': dataframe.where(dataframe['error'] != 'None').count()}


def counts(product_dataframes):
    """Generates and returns success and error counts per product

    Args:
        product_dataframes (dict) - {product:dataframe}

    Returns:
        dict - {product:{'success':count, 'error':count}}
    """
    pds = product_dataframes
    return {p: count(df) for pd in pds for p, df in pd.items()}
