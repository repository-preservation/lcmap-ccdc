import ccd
import json
import hashlib

from datetime import datetime
from functools import partial
from pandas import to_datetime
from pyspark import SparkConf
from pyspark import SparkContext

import firebird as fb
from firebird import aardvark as a
from firebird import cassandra as cass
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
     'quality': 'http://host/v1/landsat/chip-specs?q=tags:qa AND tags:pixel'}
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
    locrods  = located_rods_by_spectra
    spectra  = rods.keys()
    location = rods[spectra[0]].keys()
    rainbow  = partial(dated_colors, spectra=rods.keys(), rods=rods)
    yield tuple((xy, add_dates(rainbow(xy), dates)) for xy in xys)


def pyccd_dates(dates):
    """ Formats the pyccd date array.
    :param dates: A sequence of date strings
    :returns: A sequence of formatted and sorted ordinal dates
    """
    return sorted([fb.dtstr_to_ordinal(d) for d in dates], reverse=True)


def csort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(fb.rsort(chips, key=lambda c: c['acquired']))


def pyccd_rdd(specs_url, chips_url, x, y, acquired):
    """
    :param specs_url: URL to the chip specs host:port/context
    :param chips_url: URL to the chips host:port/context
    :param x: x coodinate contained within the extents of a pyccd rdd (chip)
    :param y: y coodinate contained within the extents of a pyccd rdd (chip)
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
    pchips = partial(a.chips, x=x, y=y, acquired=acquired)

    # get all the specs, ubids, chips, intersecting dates and rods
    specs = {k: a.chip_specs(v) for k,v in a.chip_spec_urls(specs_url).items()}

    ubids = {k: a.ubids(v) for k,v in specs.items()}

    chips = {k: csort(pchips(url=chips_url, ubids=u)) for k,u in ubids.items()}

    dates = a.intersection(map(a.dates, [c for c in chips.values()]))

    locs  = chip.locations(*chip.snap(x, y, specs[0]), specs[0]) # first is ok

    rods  = {k: to_rod(v, dates, specs[k]) for k,v in chips.items()}

    del chips

    rods  = {k: a.locrods(locs, r) for k,r in rods.items()}

    yield to_pyccd(rods, pyccd_dates(dates))


def simplify_detect_results(results):
    ''' Convert child objects inside CCD results from NamedTuples to Dictionaries '''
    output = dict()
    for key in results.keys():
        output[key] = fb.simplify_objects(results[key])
    return output


def detect(column, row, bands, chip_x, chip_y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    output = cass.RESULT_INPUT.copy()
    try:
        _results = ccd.detect(blues    = bands['blue'].values[:, row, column],
                              greens   = bands['green'].values[:, row, column],
                              reds     = bands['red'].values[:, row, column],
                              nirs     = bands['nir'].values[:, row, column],
                              swir1s   = bands['swir1'].values[:, row, column],
                              swir2s   = bands['swir2'].values[:, row, column],
                              thermals = bands['thermal'].values[:, row, column],
                              quality  = bands['cfmask'].values[:, row, column],
                              dates    = [fb.dtstr_to_ordinal(str(to_datetime(i)), False) for i in bands['dates']])
        output['result'] = json.dumps(simplify_detect_results(_results))
        output['result_ok'] = True
        output['algorithm'] = _results['algorithm']
        output['chip_x'] = chip_x
        output['chip_y'] = chip_y
    except Exception as e:
        fb.logger.error("Exception running ccd.detect: {}".format(e))
        output['result'] = ''
        output['result_ok'] = False

    output['x'] = chip_x + (column * fb.X_PIXEL_DIM)
    output['y'] = chip_y + (row * fb.Y_PIXEL_DIM)
    output['result_md5'] = hashlib.md5(output['result'].encode('UTF-8')).hexdigest()
    output['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    output['inputs_md5'] = 'not implemented'
    # writes to cassandra happen from node doing the work
    # don't want to collect all chip records on driver host
    cass.execute(cass.INSERT_CQL, [output])
    return output


def run(acquired, ulx, uly, lrx, lry, ord_date,
        lastchange=False, changemag=False, changedate=False, seglength=False, qa=False):
    '''
    Primary function for the driver module in lcmap-firebird.  Default behavior is to generate ALL the level2 products, unless specific products are requested.
    :param acquired: Date values for selecting input products. ISO format, joined with '/': <start date>/<end date> .
    :param ulx: Upper left x coordinate for area to generate CCD and Level2 product results.
    :param uly: Upper left y coordinate for area to generate CCD and Level2 product results.
    :param lrx: Lower right x coordinate for area to generate CCD and Level2 product results.
    :param lry: Lower right y coordinate for area to generate CCD and Level2 product results.
    :param ord_date: Ordinal date for which to generate Level2 products.
    :param lastchange: Generate lastchange product.
    :param changemag: Generate changemag product.
    :param changedate: Generate changedate product.
    :param seglength: Generate seglength product.
    :param qa: Generate QA product
    :return: True
    '''
    # if we can't get our Spark ducks in a row, no reason to continue
    try:
        conf = (SparkConf().setAppName("lcmap-firebird-{}".format(datetime.now().strftime('%Y-%m-%d-%I:%M')))
                .setMaster(fb.SPARK_MASTER)
                .set("spark.mesos.executor.docker.image", fb.SPARK_EXECUTOR_IMAGE)
                .set("spark.executor.cores", fb.SPARK_EXECUTOR_CORES)
                .set("spark.mesos.executor.docker.forcePullImage", fb.SPARK_EXECUTOR_FORCE_PULL))
        sc = SparkContext(conf=conf)
    except Exception as e:
        fb.logger.info("Exception creating SparkContext: {}".format(e))
        raise e

    # validate arguments
    if not valid.acquired(acquired):
        raise Exception("Invalid acquired param: {}".format(acquired))

    if not valid.coords(ulx, uly, lrx, lry):
        raise Exception("Bounding coords appear invalid: {}".format((ulx, uly, lrx, lry)))

    if not valid.ord(ord_date):
        raise Exception("Invalid ordinal date value: {}".format(ord_date))

    try:
        # organize required data
        band_queries = pyccd_chip_spec_queries(fb.SPECS_URL)
        # assuming chip-specs are identical for a location across the bands
        chipids = chip.ids(ulx, uly, lrx, lry, a.chip_specs(band_queries['blue']))

        for ids in chipids:
            # ccd results by chip
            #ccd_data = assemble_ccd_data(ids, acquired, band_queries)
            # spark prefers task sizes of 100kb or less, means ccd results results in a 1 per work bucket rdd
            # still based on the assumption of 100x100 pixel chips
            # ccd_rdd = sc.parallelize(ccd_data, 10000)
            pyccd_inputs = pyccd_rdd(fb.SPECS_URL, fb.CHIPS_URL, *ids, acquired)
            ccd_rdd = sc.parallelize(pyccd_inputs, 10000)
            for x_index in range(0, 100):
                for y_index in range(0, 100):
                    ccd_map = ccd_rdd.map(lambda i: detect(x_index, y_index, i, i[0][0], i[0][1])).persist()
                    if {False} == {lastchange, changemag, changedate, seglength, qa}:
                        # if you didn't specify, you get everything
                        ccd_map.foreach(lambda i: products.run('all', i, ord_date))
                    else:
                        if lastchange:
                            ccd_map.foreach(lambda i: products.run('lastchange', i, ord_date))
                        if changemag:
                            ccd_map.foreach(lambda i: products.run('changemag', i, ord_date))
                        if changedate:
                            ccd_map.foreach(lambda i: products.run('changedate', i, ord_date))
                        if seglength:
                            ccd_map.foreach(lambda i: products.run('seglength', i, ord_date))
                        if qa:
                            ccd_map.foreach(lambda i: products.run('qa', i, ord_date))

    except Exception as e:
        fb.logger.info("Exception running Spark job: {}".format(e))
        raise e
    finally:
        # make sure to stop the SparkContext
        sc.stop()
    return True
