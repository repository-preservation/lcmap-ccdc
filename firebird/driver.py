from firebird import aardvark as a
from firebird import dates as fd
from firebird import datastore as ds
from firebird import chip
from firebird import rdds
from firebird import validation
from functools import partial

import firebird as fb
import hashlib
import json

def chip_spec_queries(url):
    """
    A map of pyccd spectra to chip-spec queries
    :param url: full url (http://host:port/context) for chip-spec endpoint
    :return: map of spectra to chip spec queries
    :example:
    >>> chip_spec_queries('http://host/v1/landsat/chip-specs')
    {'reds':     'http://host/v1/landsat/chip-specs?q=tags:red AND sr',
     'greens':   'http://host/v1/landsat/chip-specs?q=tags:green AND sr'
     'blues':    'http://host/v1/landsat/chip-specs?q=tags:blue AND sr'
     'nirs':     'http://host/v1/landsat/chip-specs?q=tags:nir AND sr'
     'swir1s':   'http://host/v1/landsat/chip-specs?q=tags:swir1 AND sr'
     'swir2s':   'http://host/v1/landsat/chip-specs?q=tags:swir2 AND sr'
     'thermals': 'http://host/v1/landsat/chip-specs?q=tags:thermal AND ta'
     'quality':  'http://host/v1/landsat/chip-specs?q=tags:pixelqa'}
    """
    return {'reds':     ''.join([url, '?q=tags:red AND sr']),
            'greens':   ''.join([url, '?q=tags:green AND sr']),
            'blues':    ''.join([url, '?q=tags:blue AND sr']),
            'nirs':     ''.join([url, '?q=tags:nir AND sr']),
            'swir1s':   ''.join([url, '?q=tags:swir1 AND sr']),
            'swir2s':   ''.join([url, '?q=tags:swir2 AND sr']),
            'thermals': ''.join([url, '?q=tags:bt AND thermal AND NOT tirs2']),
            'quality':  ''.join([url, '?q=tags:pixelqa'])}

#:param bounds: Upper left x coordinate of area to generate products

def init(acquired, chip_ids, products, product_dates, sparkcontext,
         chips_fn=a.chips,
         specs_fn=a.chip_specs,
         clip_box=None,
         initial_partitions=fb.INITIAL_PARTITION_COUNT,
         product_partitions=fb.PRODUCT_PARTITION_COUNT):

    '''
    Constructs product graph and prepares Spark for execution
    :param acquired: Date values for selecting input products.
                     ISO format, joined with '/': <start date>/<end date>.
    :param chip_ids: Sequence of chip_ids (x, y)
    :param clip_box: Clip outputs that only fit within the supplied box.  If
                     None, no clipping is performed and full chips are produced.
    :param products: A sequence of product names to deliver
    :param product_dates:  A sequence of iso format product dates to deliver
    :param chips_fn: Function to return chips: chips(url, x, y, acquired, ubids)
    :param specs_fn: Function to return specs: chip_specs(query)
    :param initial_partitions: Number of partitions for initial query
    :param product_partitions: Number of partitions for product generation
    :param sparkcontext: A SparkContext
    :return: True
    '''
    # right now we are accepting bounds.  The bounds may be a 1 to N points.
    # This allows us to request any arbitrary area to process, even polygons.
    # We minbox the geometry and run that.  If clip is True we also don't
    # process locations that dont fit within the requested bounds.  This allows
    # us to run just a handful of pixels for evaluation rather than an entire
    # chip.
    #
    # The save functionality will allow us to save to iwds and/or to local
    # files.  This should help speed up evaluation of algorithm changes.
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
                        bounds=bounds,
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
        jobconf = fb.broadcast({'acquired': acquired,
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

        fb.logger.info('Initializing product graph:{}'\
                       .format({k:v.value for k,v in jobconf.items()}))

        graph = rdds.products(jobconf, sparkcontext)

        fb.logger.info('Product graph created:{}'\
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


def temp(bounds):
    # fb.minbox(bounds)
    spec = a.chip_specs(driver.chip_spec_queries(fb.CHIPS_URL))
    ids = chip.ids(ulx=fb.minbox(bounds)['ulx'],
                   uly=fb.minbox(bounds)['uly'],
                   lrx=fb.minbox(bounds)['lrx'],
                   lry=fb.minbox(bounds)['lry'],
                   chip_spec=spec)


def products():
    pass


def train():
    pass


def classify():
    pass


def download():
    pass


def save(products, product_dates, directory=None, iwds=False, sparkcontext=fb.sparkcontext):
    sc = sparkcontext()

    pass


#def save(graph, directory, iwds, sparkcontext=fb.sparkcontext):
#     try:

#         datastore = partial(jobconf=jobconf, sparkcontext=sparkcontext)
#         filestore = partial(jobconf=jobconf, sparkcontext=sparkcontext)

#         datastore.save(graph) if iwds is True
#         filestore.save(graph) if directory == directory_something

#         [graph[p].foreach(datastore.save) for p in products]
#         [graph[p].foreach(filestore.save) for p in products]

         #graph['products'][p].toLocalIterator()(partial(files.append(path='/directory/product-partition.txt'))) for p in products
#     finally:
         # make sure to stop the SparkContext
#         if sparkcontext is not None:
#             sparkcontext.stop()
