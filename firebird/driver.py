import firebird, ccd, argparse, json, hashlib
from firebird import products
from pandas import to_datetime
from firebird  import SPARK_MASTER, SPARK_EXECUTOR_IMAGE, SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_FORCE_PULL
from firebird  import AARDVARK_SPECS_URL
from firebird  import simplify_objects, dtstr_to_ordinal
from .validation import *
from .aardvark import pyccd_tile_spec_queries, chip_specs
from .chip     import ids as chip_ids
from pyspark   import SparkConf, SparkContext
from datetime  import datetime
from .cassandra import insert_statement
from .cassandra import execute as cassandra_execute
from .cassandra import RESULT_INPUT

# to be replaced by dave's official function names
#
from .aardvark import spicey_meatball, spicey_meatball_dates
#
#

parser = argparse.ArgumentParser(description="Driver for LCMAP product generation")
# could get cute here and loop this, leaving unwound for now
parser.add_argument('-ord', dest='ord_date')
parser.add_argument('-acq', dest='acquired')
parser.add_argument('-ulx', dest='ulx')
parser.add_argument('-uly', dest='uly')
parser.add_argument('-lrx', dest='lrx')
parser.add_argument('-lry', dest='lry')
parser.add_argument('-lastchange', dest='lastchange', action='store_true')
parser.add_argument('-changemag',  dest='changemag',  action='store_true')
parser.add_argument('-changedate', dest='changedate', action='store_true')
parser.add_argument('-seglength',  dest='seglength',  action='store_true')
parser.add_argument('-qa',         dest='qa',         action='store_true')

args = parser.parse_args()


def simplify_detect_results(results):
    ''' Convert child objects inside CCD results from NamedTuples to Dictionaries '''
    output = dict()
    for key in results.keys():
        output[key] = simplify_objects(results[key])
    return output


def detect(input, tile_x, tile_y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    # input is a tuple: ((pixel x, pixel y), {bands dict}
    _px, _py = input[0][0], input[0][1]
    _bands   = input[1]
    output = RESULT_INPUT.copy()
    try:
        # ccd switch back to using kwargs, right?
        _results = ccd.detect(blues    = _bands['blue'],
                              greens   = _bands['green'],
                              reds     = _bands['red'],
                              nirs     = _bands['nir'],
                              swir1s   = _bands['swir1'],
                              swir2s   = _bands['swir2'],
                              thermals = _bands['thermal'],
                              quality  = _bands['cfmask'],
                              dates    = [dtstr_to_ordinal(str(to_datetime(i)), False) for i in _bands['dates']])
        output['result'] = json.dumps(simplify_detect_results(_results))
        output['result_ok'] = True
        output['algorithm'] = _results['algorithm']
        output['tile_x'] = tile_x
        output['tile_y'] = tile_y
    except Exception as e:
        firebird.logger.error("Exception running ccd.detect: {}".format(e))
        output['result'] = ''
        output['result_ok'] = False

    output['x'], output['y'] = _px, _py
    output['result_md5'] = hashlib.md5(output['result'].encode('UTF-8')).hexdigest()
    output['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    output['inputs_md5'] = 'not implemented'
    # writes to cassandra happen from node doing the work
    # don't want to collect all chip records on driver host
    cassandra_execute([insert_statement(output)])
    return output


def assemble_ccd_data(tile_coords, acquired, band_queries):
    '''
    Gather data necessary for input into the CCD algorithm
    :param tile_coords: Coordinates of tiles to acquire data for
    :param acquired: Start and end dates for querying data
    :param band_queries: The actual queries used to retrieve each bands UBIDs
    :return: A dictionary of band data for a given chip of landsat data.
    '''
    ccd_data = {}
    for band in band_queries:
        ccd_data[band] = spicey_meatball(tile_coords, acquired, band_queries[band])
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
        band_queries = pyccd_tile_spec_queries(AARDVARK_SPECS_URL)
        # assuming chip-specs are identical for a location across the bands
        chipids = chip_ids(ulx, uly, lrx, lry, chip_specs(band_queries['blue']))

        for ids in chipids:
            # ccd results by chip
            ccd_data = assemble_ccd_data(ids, acquired, band_queries)
            # spark prefers task sizes of 100kb or less, means ccd results results in a 1 per work bucket rdd
            # still based on the assumption of 100x100 pixel chips
            ccd_rdd = sc.parallelize(ccd_data, 10000)
            # cache the results with persist()
            ccd_map = ccd_rdd.map(lambda i: detect(i, i[0][0], i[0][1])).persist()
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

if __name__ == "__main__":
    run(args.acquired, args.ulx, args.uly, args.lrx, args.lry, args.ord_date,
        args.lastchange, args.changemag, args.changedate, args.seglength, args.qa)
