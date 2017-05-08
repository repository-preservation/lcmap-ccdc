import firebird, ccd, json, hashlib
from firebird import products
from pandas import to_datetime
from firebird  import SPARK_MASTER, SPARK_EXECUTOR_IMAGE, SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_FORCE_PULL
from firebird  import AARDVARK_SPECS_URL, X_PIXEL_DIM, Y_PIXEL_DIM
from firebird  import simplify_objects, dtstr_to_ordinal
from .validation import *
from .aardvark import pyccd_chip_spec_queries, chip_specs
from .chip     import ids as chip_ids
from pyspark   import SparkConf, SparkContext
from datetime  import datetime
from .cassandra import execute as cassandra_execute
from .cassandra import RESULT_INPUT
from .cassandra import INSERT_CQL

import aardvark as a
#from aardvark import chip_specs
#from aardvark import byubid
#from aardvark import ubid
#from aardvark import chips
#from aardvark import sort
#from aardvark import dates
#from aardvark import intersection
#from aardvark import trim
#from aardvark import to_numpy
#from aardvark import rods
#from aardvark import locrods

from chip import locations
from chip import snap

from functools import partial


# to be replaced by dave's official function names
#
from .aardvark import spicey_meatball, spicey_meatball_dates
#
#


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
            'quality': ''.join([url, '?q=tags:pixelqa'])}


def query(x, y, acquired, chips_endpoint, chip_spec_endpoint):
    """ Retrieves chips and chip_specs for the supplied parameters """
    return {'chips': a.sort(a.chips(chips_endpoint, x, y, acquired, ubids)),
            'specs': chip_specs(chip_spec_endpoint)}


def transform(data, common_dates, locations):
    return compose(partial(a.trim, common_dates=common_date),
                   partial(a.to_numpy, chip_specs_byubid=a.byubid(data['specs']),
                   a.rods,
                   partial(a.locrods, locations=locations))

               
def transform(data, common_dates, locations):
    """ Mutate the data dictionary chips with the proper transformations to
    generate a dictionary of rods with location information.
    :param data: dict containing 'specs' and 'chips' keys
    :common_dates: Set of dates to include in transformed output
    :returns: A tuple of dates and rods """
    data['chips'] = a.trim(data['chips'], common_dates)
    data['chips'] = a.to_numpy(data['chips'], a.byubid(data['specs']))
    data['rods'] = a.rods(data['chips'])
    data['rods'] = a.locrods(locations, data['chips']))
    data.pop('chips') if 'chips' in data
    data.pop('chip_specs') if 'chip_specs' in data

    return firebird.dtstr_to_ordinal(a.dates(data['chips'])), a.locrods(locations, data['chips'])


def rods_by_xy(dates, xys, rods):

    def colors(spectra, rods, xy):
        return {spec: rods[spec][xy] for spec in spectra}

    rainbow = partial(colors, spectra=rods.keys(), rods=rods)
    yield rainbow(xy)) for xy in xys

    tuple([xy, {'dates': dates}.update((xy) for xy in xys)}])


def pyccd_rdd(chip_specs_endpoint, chips_endpoint, x, y, acquired):

    pquery = partial(query, x=x, y=y, acquired=acquired,
                     chips_endpoint=chips_endpoint)

    # get all the query urls
    urls = chip_spec_urls(chip_specs_endpoint)

    # query for data (chips + chip_specs) organized by spectral key
    # TODO: parallelizable by spectra if desired
    data = {spectra: pquery(url) for spectra, url in urls.iteritems}

    # find the common dates for all chips in all spectra
    common_dates = a.intersection(map(a.dates, data.values()['chips']))

    # just pick the first chip spec for now.  If the chips are not symmetrical
    # then there are big problems with data ingest.
    # This will need to be made more robust later if each chip doesn't not have
    # the same geometry and resolution
    fspec = data['specs'][0]
    locations = chip.locations(*chip.snap(x, y, fspec), fspec)

    # transform each spectra to rods with locations
    # this is a dict of dicts:
    # {spectra: {'dates': [date1, date2, date3],
    #            'rods': {(x1,y1):[ts1, ts2], (x2,y2):[ts1, ts2]]]}
    data = {k, transform(v, common_dates, locations)) for k, v in data}

    # perform final transformation, emitting a list of dictionaries
    # with a rod for each spectra plus a date array organized by x,y
    # ((x, y): {'dates': [], 'reds': [], 'greens': [],
    #           'blues': [], 'nirs1': [], 'swir1s': [],
    #           'swir2s': [], 'thermals': [], quality: []},)
    xys = data[spectra[0]]['rods'].keys()
    dates = data[spectra[0]]['dates']
    yield rods_by_xy(dates, xys, locrods)


def simplify_detect_results(results):
    ''' Convert child objects inside CCD results from NamedTuples to Dictionaries '''
    output = dict()
    for key in results.keys():
        output[key] = simplify_objects(results[key])
    return output


def detect(column, row, bands, chip_x, chip_y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    output = RESULT_INPUT.copy()
    try:
        _results = ccd.detect(blues    = bands['blue'].values[:, row, column],
                              greens   = bands['green'].values[:, row, column],
                              reds     = bands['red'].values[:, row, column],
                              nirs     = bands['nir'].values[:, row, column],
                              swir1s   = bands['swir1'].values[:, row, column],
                              swir2s   = bands['swir2'].values[:, row, column],
                              thermals = bands['thermal'].values[:, row, column],
                              quality  = bands['cfmask'].values[:, row, column],
                              dates    = [dtstr_to_ordinal(str(to_datetime(i)), False) for i in bands['dates']])
        output['result'] = json.dumps(simplify_detect_results(_results))
        output['result_ok'] = True
        output['algorithm'] = _results['algorithm']
        output['chip_x'] = chip_x
        output['chip_y'] = chip_y
    except Exception as e:
        firebird.logger.error("Exception running ccd.detect: {}".format(e))
        output['result'] = ''
        output['result_ok'] = False

    output['x'] = chip_x + (column * X_PIXEL_DIM)
    output['y'] = chip_y + (row * Y_PIXEL_DIM)
    output['result_md5'] = hashlib.md5(output['result'].encode('UTF-8')).hexdigest()
    output['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    output['inputs_md5'] = 'not implemented'
    # writes to cassandra happen from node doing the work
    # don't want to collect all chip records on driver host
    cassandra_execute(INSERT_CQL, [output])
    return output


def assemble_ccd_data(chip_coords, acquired, band_queries):
    '''
    Gather data necessary for input into the CCD algorithm
    :param chip_coords: Coordinates of chips to acquire data for
    :param acquired: Start and end dates for querying data
    :param band_queries: The actual queries used to retrieve each bands UBIDs
    :return: A dictionary of band data for a given chip of landsat data.
    '''
    ccd_data = {}
    for band in band_queries:
        ccd_data[band] = spicey_meatball(chip_coords, acquired, band_queries[band])
    # toss this over the fence to aardvark for now
    ccd_data['dates'] = spicey_meatball_dates(ccd_data['blue'])
    return ccd_data


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
                .setMaster(SPARK_MASTER)
                .set("spark.mesos.executor.docker.image", SPARK_EXECUTOR_IMAGE)
                .set("spark.executor.cores", SPARK_EXECUTOR_CORES)
                .set("spark.mesos.executor.docker.forcePullImage", SPARK_EXECUTOR_FORCE_PULL))
        sc = SparkContext(conf=conf)
    except Exception as e:
        firebird.logger.info("Exception creating SparkContext: {}".format(e))
        raise e

    # validate arguments
    if not valid_acquired(acquired):
        raise Exception("Invalid acquired param: {}".format(acquired))

    if not valid_coords(ulx, uly, lrx, lry):
        raise Exception("Bounding coords appear invalid: {}".format((ulx, uly, lrx, lry)))

    if not valid_ord(ord_date):
        raise Exception("Invalid ordinal date value: {}".format(ord_date))

    try:
        # organize required data
        band_queries = pyccd_chip_spec_queries(AARDVARK_SPECS_URL)
        # assuming chip-specs are identical for a location across the bands
        chipids = chip_ids(ulx, uly, lrx, lry, chip_specs(band_queries['blue']))

        for ids in chipids:
            # ccd results by chip
            ccd_data = assemble_ccd_data(ids, acquired, band_queries)
            # spark prefers task sizes of 100kb or less, means ccd results results in a 1 per work bucket rdd
            # still based on the assumption of 100x100 pixel chips
            ccd_rdd = sc.parallelize(ccd_data, 10000)
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
        firebird.logger.info("Exception running Spark job: {}".format(e))
        raise e
    finally:
        # make sure to stop the SparkContext
        sc.stop()
    return True
