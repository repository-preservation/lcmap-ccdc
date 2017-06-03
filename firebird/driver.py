from firebird import aardvark as a
from firebird import date as fd
from firebird import datastore
from firebird import chip
from firebird import products
from firebird import validation as valid
from functools import partial

import firebird as fb
import hashlib
import json

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
    :returns: List of tuples
    :example:
    located_rods_by_spectra parameter:
    {'red':   {(0, 0): [110, 110, 234, 664], (0, 1): [23, 887, 110, 111]}}
    {'green': {(0, 0): [120, 112, 224, 624], (0, 1): [33, 387, 310, 511]}}
    {'blue':  {(0, 0): [128, 412, 244, 654], (0, 1): [73, 987, 119, 191]}}
    ...

    returns:
    (((0, 0), {'red':   [110, 110, 234, 664],
               'green': [120, 112, 224, 624],
               'blue':  [128, 412, 244, 654], ... }),
     ((0, 1), {'red':   [23, 887, 110, 111],
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
    return tuple([(xy, add_dates(rainbow(xy=xy), dates)) for xy in locations])


def pyccd_dates(dates):
    """ Formats the pyccd date array.
    :param dates: A sequence of date strings
    :returns: A sequence of formatted and sorted ordinal dates
    """
    return fb.rsort([fd.to_ordinal(d) for d in dates])


def csort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(fb.rsort(chips, key=lambda c: c['acquired']))


def pyccd_inputs(point, specs_url, specs_fn, chips_url, chips_fn, acquired):
    """
    :param point: A tuple of (x, y) which is within the extents of a chip
    :param specs_url: URL to the chip specs host:port/context
    :param specs_fn:  Function that accepts a url query and returns chip specs
    :param chips_url: URL to the chips host:port/context
    :param chips_fn:  Function that accepts x, y, acquired, url, ubids and
                      returns chips.
    :param acquired: Date range string as start/end, ISO 8601 date format
    :returns: A tuple of tuples.
    (((x1, y1), {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []}),
     ((x1, y2), {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []}))
    """
    # create a partial function initialized with x, y and acquired since
    # those are static for this call to pyccd_rdd
    #pchips = partial(a.chips, x=point[0], y=point[1], acquired=acquired)
    pchips = partial(chips_fn, x=point[0], y=point[1], acquired=acquired)

    # get all the specs, ubids, chips, intersecting dates and rods
    # keep track of the spectra they are associated with via dict key 'k'
    #specs = {k: a.chip_specs(v) for k, v in chip_spec_urls(specs_url).items()}
    specs = {k: specs_fn(v) for k, v in chip_spec_urls(specs_url).items()}

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


def points_filter(value, bbox, enforce):
    '''
    Determines if a point value fits within a bounding box (edges inclusive)
    Useful as a filtering function with conditional enforcement.

    :param value: Tuple: ((x,y), (data))
    :param bbox: dict with keys: ulx, uly, lrx, lry
    :param enforce: Boolean, whether to apply this function or not.
    :return: Boolean
    '''
    def fits(point, bbox):
        x,y = point
        return float(x) >= float(bbox['ulx']) and\
               float(x) <= float(bbox['lrx']) and\
               float(y) >= float(bbox['lry']) and\
               float(y) <= float(bbox['uly'])

    return fb.false(enforce) or fits(value[0], bbox)


def products_graph(jobconf, sparkcontext):
    """ Product graph for firebird products
    :param jobconf: dict of broadcast variables
    :param sparkcontext: Configured spark context
    :return: dict keyed by product with lazy RDD as value
    """
    jc = jobconf

    chipids = sparkcontext.parallelize(jc['chipids'].value,
                                       jc['chipid_partitions'].value)\
                                      .setName("CHIP IDS").persist()

    # query data and transform it into pyccd input format
    inputs = chipids.map(partial(pyccd_inputs,
                                 specs_url=jc['specs_url'].value,
                                 specs_fn=jc['specs_fn'].value,
                                 chips_url=jc['chips_url'].value,
                                 chips_fn=jc['chips_fn'].value,
                                 acquired=jc['acquired'].value))\
                                 .flatMap(lambda x: x)\
                                 .filter(partial(points_filter,
                                                 bbox=jc['clip_box'].value,
                                                 enforce=jc['clip'].value))\
                                 .repartition(jc['product_partitions'].value)\
                                 .setName('PYCCD INPUTS').persist()


    ccd = inputs.map(products.ccd).setName('CCD').persist()

    # TODO: how are we going to generate multiple products based on a
    # sequence of dates?  Products function might need to accept the list of
    # dates and return proper rdd'd data structure with
    # ((x, y, algorithm, product_date_str), data) in order to be saveable

    lastchange = ccd.mapValues(partial(products.lastchange,
                                       ord_date=jc['product_dates'].value))\
                                       .setName("LASTCHANGE")

    changemag  = ccd.mapValues(partial(products.changemag,
                                       ord_date=jc['product_dates'].value))\
                                       .setName('CHANGEMAG')

    changedate = ccd.mapValues(partial(products.changedate,
                                       ord_date=jc['product_dates'].value))\
                                       .setName('CHANGEDATE')

    seglength = ccd.mapValues(partial(products.seglength,
                                      ord_date=jc['product_dates'].value,
                                      bot=fd.startdate(jc['acquired'].value)))\
                                      .setName('SEGLENGTH')

    curveqa = ccd.mapValues(partial(products.curveqa,
                                    ord_date=jc['product_dates'].value))\
                                    .setName("CURVEQA")

    return {'chipids': chipids,
            'inputs': inputs,
            'ccd': ccd,
            'lastchange': lastchange,
            'changemag': changemag,
            'seglength': seglength,
            'curveqa': curveqa}


def broadcast(context, sparkcontext):
    '''
    Sets variables on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    '''
    return {k: sparkcontext.broadcast(v) for k,v in context.items()}


def save(products, products_rdd):
    return [products_rdd[p].foreach(datastore.save) for p in products]


def run(acquired, bounds, products, product_dates, clip, chips_fn=a.chips,
        specs_fn=a.chip_specs, sparkcontext=fb.sparkcontext):

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
    # raises appropriate exceptions on error
    validation.validate(acquired=acquired,
                        bounds=bounds,
                        products=products,
                        product_dates=product_dates,
                        clip=clip,
                        chips_fn=chips_fn,
                        specs_fn=specs_fn)

    try:
        # retrieve a chip spec so we can generate chip ids
        spec = specs_fn(chip_spec_urls(fb.SPECS_URL)['blues'])[0]

        # put the values we need on the cluster to make them available
        # to distributed functions.  They're in a dictionary to help
        # differentiate variables that have not been broadcast and to make
        # a testable function out of it
        # Broadcast variables are read-only on every node and must fit
        # in memory.
        jobconf = broadcast({'acquired': acquired,
                             'bbox': fb.minbox(bounds),
                             'chip_ids': chip.ids(ulx=fb.minbox(bounds)['ulx'],
                                                  uly=fb.minbox(bounds)['uly'],
                                                  lrx=fb.minbox(bounds)['lrx'],
                                                  lry=fb.minbox(bounds)['lry'],
                                                  chip_spec=spec),
                             'chipid_partitions': 10,
                             'chips_fn': chips_fn,
                             'chips_url': fb.CHIPS_URL,
                             'clip': clip,
                             'products': products,
                             'product_dates': product_dates,
                             'product_partitions': 2000,
                              # should be able to pull this from the
                              # specs_fn and specs_url but this lets us
                              # do it once without beating aardvark up.
                              'reference_spec': spec,
                              'specs_url': fb.SPECS_URL,
                              'specs_fn': specs_fn},
                             sparkcontext=sparkcontext)

        fb.logger.info('Initializing product graph:\n{}'\
                       .format({k:v.value for k,v in jobconf}))

        # everything from here down is an RDD/broadcast variable/cluster op.
        # Don't mix up driver memory locations and cluster memory locations
        # chipid_rdd accepts a non broadcast set of variables because it is
        # the initial RDD.
        products_rdds = products_graph(jobconf, sparkcontext)

        fb.logger.info('Product graph created:\n{}'\
                       .format(products_rdds.keys()))

        # product call graphs are created but not realized.  Do something with
        # whichever one you want in order to cause the computation to occur
        # (example: if curveqa is requested, save it and it will compute)
        # how am i going to get a cassandra connection from each node without
        # creating 10,000 connections?
        return save(products, products_rdds)

    except Exception as e:
        fb.logger.info("Exception generating firebird products: {}".format(e))
        raise e
    finally:
        # make sure to stop the SparkContext
        if sparkcontext is not None:
            sparkcontext.stop()
    return True
