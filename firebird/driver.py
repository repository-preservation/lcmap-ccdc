import ccd
import hashlib
import json

from datetime import datetime
from functools import partial

import firebird as fb
from firebird import aardvark as a
from firebird import datastore as cass
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
     'quality': 'http://host/v1/landsat/chip-specs?q=tags:pixelqa'}
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
    """Organizes rods by xy instead of by spectrum
    :param located_rods_by_spectra: dict of dicts, keyed first by spectra
                                    then by coordinate
    :returns: Generated tuple of tuples
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


def run(acquired, ulx, uly, lrx, lry, prod_date, ccd=False,
        lastchange=False, changemag=False, changedate=False, seglength=False,
        qa=False, sparkcontext=fb.sparkcontext):
    '''
    Primary function for the driver module in lcmap-firebird.
    Default behavior is to generate ALL the products
    :param acquired: Date values for selecting input products.
                     ISO format, joined with '/': <start date>/<end date>.
    :param ulx: Upper left x coordinate of area to generate products
    :param uly: Upper left y coordinate for area to generate products
    :param lrx: Lower right x coordinate for area to generate products
    :param lry: Lower right y coordinate for area to generate products
    :param ord_date: Ordinal date for which to generate products
    :param lastchange: Generate lastchange product.
    :param changemag: Generate changemag product.
    :param changedate: Generate changedate product.
    :param seglength: Generate seglength product.
    :param qa: Generate QA product.
    :param sparkcontext: Function which returns a SparkContext object
    :return: True
    '''

    # Get the SparkContext
    sc = sparkcontext()

    # validate arguments
    if not valid.acquired(acquired):
        raise Exception("Acquired param is invalid: {}".format(acquired))

    if not valid.coords(ulx, uly, lrx, lry):
        raise Exception("invalid bbox: {}".format((ulx, uly, lrx, lry)))

    if not valid.prod(prod_date):
        raise Exception("Invalid product date value: {}".format(prod_date))

    try:
        # retrieve a chip spec so we can generate chip ids
        spec = a.specs(chip_spec_urls(fb.SPECS_URL)['blues'])[0]
        cids = chip.ids(ulx, uly, lrx, lry, spec)

        # everything from here down is an RDD or broadcast variable.
        # Don't mix up driver memory locations with what's on the cluster.
        chips_url    = sc.broadcast(fb.CHIPS_URL)
        specs_url    = sc.broadcast(fb.SPECS_URL)
        acquired     = sc.broadcast(acquired)
        spec         = sc.broadcast(spec)
        product_date = sc.broadcast(fb.dtstr_to_ordinal(prod_date))

        # create chip id RDD so we can parallelize data query and prep
        chip_ids = sc.parallelize(cids, partition_count, len(cids)))

        # query data and transform it into pyccd input format
        inputs = chip_ids.map(partial(pyccd_inputs,
                                      specs_url=specs_url,
                                      chips_url=chips_url,
                                      acquired=acquired)).persist()

        # repartition to achieve maximum parallelism
        ccd_inputs = inputs.repartition(len(inputs))

        # create product rdds
        detected   = ccd_inputs.mapValues(products.ccd).persist()

        lastchange = detected.mapValues(partial(products.lastchange,
                                                ord_date=product_date))
        changemag  = detected.mapValues(partial(products.changemag,
                                                ord_date=product_date))
        changedate = detected.mapValues(partial(products.changedate,
                                                ord_date=product_date))
        seglength  = detected.mapValues(partial(products.seglength,
                                                ord_date=product_date,
                                                bot=))
        qa         = detected.mapValues(partial(products.qa,
                                                ord_date=product_date))


    except Exception as e:
        fb.logger.info("Exception running Spark job: {}".format(e))
        raise e
    finally:
        # make sure to stop the SparkContext
        sc.stop()
    return True
