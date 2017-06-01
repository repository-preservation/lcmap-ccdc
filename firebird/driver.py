import hashlib
import json

from datetime import datetime
from functools import partial

import firebird as fb
from firebird import aardvark as a
from firebird import datastore
from firebird import chip
from firebird import products
from firebird import validation as valid


def chip_spec_urls(url):
    """
    A map of pyccd spectra to chip-spec queries
    :param url: full url (http://host:port/context) for chip-spec endpoint
    :return: map of spectra to chip spec queries
    :example:
    >>> pyccd_chip_spec_queries('http://host/v1/landsat/chip-specs')
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


def to_rod(chips, dates, specs):
    """ Function to convert chips to rods.
        Exists primarily to clean up syntax """
    return a.rods(a.to_numpy(a.trim(chips, dates), a.byubid(specs)))


def to_pyccd(located_rods_by_spectra, dates):
    """ Organizes rods by xy instead of by spectrum
    :param located_rods_by_spectra: dict of dicts, keyed first by spectra
                                    then by coordinate
    :returns: Tuple of tuples
    :example:
    located_rods_by_spectra parameter:
    {'red':   {(0, 0): [110, 110, 234, 664], (0, 1): [23, 887, 110, 111]}}
    {'green': {(0, 0): [120, 112, 224, 624], (0, 1): [33, 387, 310, 511]}}
    {'blue':  {(0, 0): [128, 412, 244, 654], (0, 1): [73, 987, 119, 191]}}
    ...

    returns:
    (((0, 0): {'red':   [110, 110, 234, 664],
               'green': [120, 112, 224, 624],
               'blue':  [128, 412, 244, 654], ... },
      (0, 1): {'red':   [23, 887, 110, 111],
               'green': [33, 387, 310, 511],
               'blue':  [73, 987, 119, 191], ...}))
    ...
    """
    def colors(spectra, rods, xy):
        return {spec: rods[spec][xy] for spec in spectra}

    def add_dates(rainbow, dates):
        rainbow['dates'] = dates
        return rainbow

    # alias the descriptive name down to something that doesn't take up a line
    locrods   = located_rods_by_spectra
    spectra   = tuple(locrods.keys())
    locations = locrods[spectra[0]].keys()
    rainbow   = partial(colors, spectra=locrods.keys(), rods=locrods)
    return tuple((xy, add_dates(rainbow(xy=xy), dates)) for xy in locations)


def pyccd_dates(dates):
    """ Formats the pyccd date array.
    :param dates: A sequence of date strings
    :returns: A sequence of formatted and sorted ordinal dates
    """
    return fb.rsort([fb.dtstr_to_ordinal(d) for d in dates])


def csort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(fb.rsort(chips, key=lambda c: c['acquired']))


def pyccd_inputs(point, specs_url, chips_url, acquired):
    """
    :param point: A tuple of (x, y) which is within the extents of a chip
    :param specs_url: URL to the chip specs host:port/context
    :param chips_url: URL to the chips host:port/context
    :param acquired: Date range string as start/end, ISO 8601 date format
    :returns: A tuple of tuples.
    (((x1, y1), {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []}),
     ((x1, y2): {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []},))
    """
    # create a partial function initialized with x, y and acquired since
    # those are static for this call to pyccd_rdd
    pchips = partial(a.chips, x=point[0], y=point[1], acquired=acquired)

    # get all the specs, ubids, chips, intersecting dates and rods
    # keep track of the spectra they are associated with via dict key 'k'
    specs = {k: a.chip_specs(v) for k, v in chip_spec_urls(specs_url).items()}

    ubids = {k: a.ubids(v) for k, v in specs.items()}

    chips = {k: csort(pchips(url=chips_url, ubids=u)) for k, u in ubids.items()}

    dates = a.intersection(map(a.dates, [c for c in chips.values()]))

    bspecs = specs['blues']

    locs = chip.locations(*chip.snap(*point, bspecs[0]), bspecs[0])

    add_loc = partial(a.locrods, locs)
    rods = {k: add_loc(to_rod(v, dates, specs[k])) for k, v in chips.items()}
    del chips

    return to_pyccd(rods, pyccd_dates(dates))


def simplify_detect_results(results):
    ''' Convert child objects inside CCD results from NamedTuples to dicts '''
    output = dict()
    for key in results.keys():
        output[key] = fb.simplify_objects(results[key])
    return output


def startdate(acquired):
    return acquired.split('/')[0]


def enddate(acquired):
    return acquired.split('/')[1]


def products_graph(chip_ids_rdd, broadcast):
    """ Product graph for firebird products
    :param chip_ids_rdd: An RDD with the chip ids to generate products
    :param broadcast: References to variables on the cluster
    :return: dict keyed by product with lazy RDD as value
    """
    bc = broadcast

    # query data and transform it into pyccd input format
    inputs = chip_ids.map(partial(pyccd_inputs,
                                  specs_url=bc['specs_url'],
                                  chips_url=bc['chips_url'],
                                  acquired=bc['acquired'])).persist()

    # repartition to achieve maximum parallelism
    ccd = inputs.repartition(len(inputs)).map(products.ccd).persist()

    # how the eff are we going to generate multiple products based on a
    # sequence of dates?

    lastchange = ccd.mapValues(partial(products.lastchange,
                                       ord_date=bc['product_date']))
    changemag  = ccd.mapValues(partial(products.changemag,
                                       ord_date=bc['product_date']))
    changedate = ccd.mapValues(partial(products.changedate,
                                       ord_date=bc['product_date']))
    seglength  = ccd.mapValues(partial(products.seglength,
                                       ord_date=bc['product_date'],
                                       bot=bc['ordinal_start_date']))
    curveqa    = ccd.mapValues(partial(products.curveqa,
                                       ord_date=bc['product_date']))

    return {'inputs': inputs,
            'ccd': ccd,
            'lastchange': lastchange,
            'changemag': changemag,
            'seglength': seglength,
            'curveqa': curveqa}


def broadcast(chip_url, specs_url, acquired, spec, product_date, clip,
              products, bbox, sparkcontext):
    """ Sets variables on the cluster to make them available to cluster
    operations.
    :param chip_url: Endpoint for chips
    :param spec_url: Endpoint for specs
    :param acquired: Date range for input data query
    :param spec: A spec representing the chip geometry
    :param product_date: Date to produce a product
    :param clip: Should product outputs be filtered to exclude points that do
                 not fall within the requested bounds. True/False
    :param products: Sequence of products that were requested
    :param bbox: Bounding box for requested area, dict with ulx, uly, lrx, lry
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each variable
    """
    sc = sparkcontext
    return {'chips_url':    sc.broadcast(chips_url),
            'specs_url':    sc.broadcast(specs_url),
            'acquired':     sc.broadcast(acquired),
            'spec':         sc.broadcast(spec),
            'product_date': sc.broadcast(product_dates),
            'start_date':   sc.broadcast(startdate(acquired)),
            'clip':         sc.broadcast(clip),
            'products':     sc.broadcast(products),
            'bbox':         sc.broadcast(bbox)}


def chipid_rdd(chip_ids, sparkcontext):
    """
    Creates and returns an RDD for chip ids
    :param chip_ids: A sequence of chip ids
    :param sparkcontext: Context pointed to a spark cluster
    :return: An RDD of chip_ids
    """
    return sparkcontext.parallelize(chip_ids, len(chip_ids))


def store(products, products_rdd):
    for p in products:
       datastore.save([x for x in products_rdd[p].toLocalIterator()])


def run(acquired, bounds, products, product_dates, clip,
        sparkcontext=fb.sparkcontext):

    '''
    Primary function for the driver module in lcmap-firebird.
    :param acquired: Date values for selecting input products.
                     ISO format, joined with '/': <start date>/<end date>.
    :param bounds: Upper left x coordinate of area to generate products
    :param clip: If True, filters out locations that do not fit within a minbox
                 of the bounds.
    :param products: A sequence of product names to deliver
    :param product_dates:  A sequence of iso format product dates to deliver
    :param sparkcontext: Function which returns a SparkContext object
    :return: True
    '''

    # validate arguments
    if not valid.acquired(acquired):
        raise Exception("Acquired dates are invalid: {}".format(acquired))

    if not valid.prod(prod_date):
        raise Exception("Invalid product date value: {}".format(prod_date))

    sc = sparkcontext

    bbox = fb.minbox(bounds)

    try:
        # retrieve a chip spec so we can generate chip ids
        spec = a.specs(chip_spec_urls(fb.SPECS_URL)['blues'])[0]

        # put the variables we need on the cluster to make them available
        # to distributed functions.  They're in a dictionary to help
        # differentiate variables that have not been broadcast and to make
        # a testable function out of it
        bc = broadcast(fb.CHIPS_URL, fb.SPECS_URL, acquired, spec,
                       product_dates, clip, products, bbox, sc)

        # everything from here down is an RDD/broadcast variable/cluster op.
        # Don't mix up driver memory locations and cluster memory locations
        # chipid_rdd can accept a non broadcast set of variables because it is
        # the initial RDD.
        products_rdds = products_graph(chipid_rdd(chip.ids(bbox['ulx'],
                                                           bbox['uly'],
                                                           bbox['lrx'],
                                                           bbox['lry'],
                                                           spec)), bc)

        # product call graphs are created but not realized.  Do something with
        # whichever one you want in order to cause the computation to occur
        # (example: if curveqa is requested, save it and it will compute)
        # how am i going to get a cassandra connection from each node without
        # creating 10,000 connections?
        return store(products, products_rdds)

    except Exception as e:
        fb.logger.info("Exception generating firebird products: {}".format(e))
        raise e
    finally:
        # make sure to stop the SparkContext
        sc.stop()
    return True
