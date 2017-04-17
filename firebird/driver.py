import firebird
from firebird import SPARK_MASTER
from firebird import LCMAP_PRODUCT_DICT
from .aardvark import data
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


def run(acquired, upperleft, lowerright, ord_date, product):
    if product not in LCMAP_PRODUCT_DICT:
        raise Exception("Invalid LCMAP product request")

    if not valid_date(ord_date):
        raise Exception("Invalid date for LCMAP product request")

    # ccd results for (acquired, upperleft, lowerright)

    # product output for (ord_date, product)

    return True

if __name__ == "__main__":
    run(args.acquired, args.upperleft, args.lowerright, args.ord_date, args.product)