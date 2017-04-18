import firebird
from firebird import SPARK_MASTER
from firebird import LCMAP_PRODUCT_DICT
from firebird import AARDVARK_SPECS_URL
from .aardvark import data
from .aardvark import pyccd_tile_spec_queries
from .aardvark import chip_specs
from .chip import ids as chip_ids
from pyspark import SparkConf, SparkContext
import ccd
import sys
import argparse

parser = argparse.ArgumentParser(description="Driver for LCMAP product generation")
parser.add_argument('-p', '--product', dest='product')
parser.add_argument('-d', '--date', dest='ord_date')
parser.add_argument('-a', '--acquired', dest='acquired')
parser.add_argument('-ul', '--upperleft', dest='upperleft')
parser.add_argument('-lr', '--lower_right', dest='lowerright')

args = parser.parse_args()


def valid_date(indate):
    return True


def run(acquired, ulx, uly, lrx, lry, ord_date, product):
    if product not in LCMAP_PRODUCT_DICT:
        raise Exception("Invalid LCMAP product request")

    if not valid_date(ord_date):
        raise Exception("Invalid date for LCMAP product request")

    # ccd results for (acquired, upperleft, lowerright)
    ## to-be-removed/replaced
    band_queries = pyccd_tile_spec_queries(AARDVARK_SPECS_URL)
    band_specs = {}
    chipids = {}
    for band in band_queries:
        band_specs[band] = chip_specs(band_queries[band])
        chipids[band] = chip_ids(ulx, uly, lrx, lry, band_specs[band])

    ## end to-be-removed

    # we have chip ids per band now, probably unnecessary
    # for every chip id, get pyccd results

    # ccd_data =

    # product output for (ord_date, product)

    return True

if __name__ == "__main__":
    run(args.acquired, args.upperleft, args.lowerright, args.ord_date, args.product)