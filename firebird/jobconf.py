from firebird.io.write import cassandra as writer
from merlin import functions as f

import firebird as fb
import logging


logger = logging.getLogger(__name__)


def broadcast(context, sparkcontext):
    """Sets read-only values on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    """
    return {k: sparkcontext.broadcast(value=v) for k, v in context.items()}


def create(acquired, chip_ids, products, product_dates, sparkcontext,
           chips_fn=chips.get,
           specs_fn=chip_specs.get,
           clip_box=None,
           initial_partitions=fb.INITIAL_PARTITION_COUNT,
           product_partitions=fb.PRODUCT_PARTITION_COUNT):
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

        dates_fn = partial(f.chexists,
                           check_fn=timeseries.symmetric_dates,
                           keys=['quality'])

        jobconf = broadcast(context={'acquired': acquired,
                                     'clip_box': clip_box,
                                     'chip_ids': chip_ids,
                                     'chips_fn': chips_fn,
                                     'chip_spec_queries': queries,
                                     'chips_url': fb.CHIPS_URL,
                                     'clip_box': clip_box,
                                     'dates_fn': dates_fn,
                                     'initial_partitions': initial_partitions,
                                     'products': products,
                                     'product_dates': product_dates,
                                     'product_partitions': product_partitions,
                                     'reference_spec': spec,
                                     'specs_fn': specs_fn},
                            sparkcontext=sparkcontext)

        logger.info('Creating jobconf ...')
        logger.debug({k: v.value for k, v in jobconf.items()})
        return jobconf

    except Exception as e:
        logger.error("Exception creating firebird jobconf: {}".format(e))
        raise e


def save(jobconf, sparksession)
        ss = sparksession

        md5, cfg = f.serialize({k: f.represent(v.value)
                               for k, v in jobconf.items()})

        dataframe = ss.createDataFrame([[md5, cfg]], ['id', 'config']))

        result = writer(table='jobconf', dataframe=dataframe)

        logger.info('Jobconf saved:{}'.format(result))

        return md5, cfg
