import click
import argparse

from firebird import driver

parser = argparse.ArgumentParser(description="Driver for LCMAP product generation")
# could get cute here and loop this, leaving unwound for now
parser.add_argument('-prd', dest='prod_date')
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


@click.group
def cli():
    print("Starting firebird...")


@cli.command
def run(acquired, ulx, uly, lrx, lry, prod_date,
        lastchange=False, changemag=False, changedate=False, seglength=False, qa=False):
    return driver.run(acquired, ulx, uly, lrx, lry, prod_date, lastchange, changemag, changedate, seglength, qa)


@cli.command
def products():
    pass


if __name__ == "__main__":
    run(args.acquired, args.ulx, args.uly, args.lrx, args.lry, args.prod_date,
        args.lastchange, args.changemag, args.changedate, args.seglength, args.qa)