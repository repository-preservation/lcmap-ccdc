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

# to be replaced by dave's official function names
#
from .aardvark import spicey_meatball, spicey_meatball_dates
#
#


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

