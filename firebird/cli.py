"""cli.py is the command line interface for Firebird.  

Prerequisites:
1. Set env vars as defined in __init__.py.
2. Pyspark must be available.  This is normally provided by the parent Docker image (lcmap-spark).
3. Chipmunk must be available on the network for ARD and AUX data.
4. Cassandra must be available to read and write pyccd, training, and classification results.

cli.py should be added to setup.py as an entry_point console script.  After installing the Firebird python package, it would then be invoked as the entrypoint of the Firebird Docker image.
"""

from cytoolz   import do
from cytoolz   import filter
from cytoolz   import first
from cytoolz   import merge
from cytoolz   import take
from firebird  import ARD
from firebird  import logger
from functools import partial
from merlin    import functions

import cassandra
import classification
import click
import firebird
import grids
import pyccd
import timeseries
import training
import traceback


def context_settings():
    """Normalized tokens for Click cli

    Returns:
        dict
    """

    return dict(token_normalize_func=lambda x: x.lower())


@click.group(context_settings=context_settings())
def entrypoint():
    """Placeholder function to group Click commands"""
    pass


@entrypoint.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
@click.option('--acquired', '-a', required=True)
@click.option('--number', '-n', required=False, default=2500)
def changedetection(x, y, acquired, number=2500):
    """Run change detection for a tile over a time range and save results to Cassandra.
ool    
    Args:
        x (int): tile x coordinate
        y (int): tile y coordinate
        acquired (str): ISO8601 date range
        number (int): Number of chips to run change detection on.  Testing only.
    Returns:
        TBD

    """
    ctx = None
    try:
        # connect to the cluster
        ctx = firebird.context('changedetection')
        
        # wire everything up
        tile = grids.tile(x=x, y=y, cfg=ARD)
        ids  = timeseries.ids(ctx=ctx, chips=take(number, tile.get('chips')))
        ard  = timeseries.ard(ctx=ctx, ids=ids, acquired=acquired).cache()        
        ccd  = pyccd.dataframe(ctx=ctx, df=ard).cache()
        
        logger(ctx, 'changedetection').info(str(merge(tile, {'acquired': acquired,
                                                             'input partitions': firebird.INPUT_PARTITIONS,
                                                             'product_partitions': firebird.PRODUCT_PARTITIONS,
                                                             'chips': ids.count()})))

        # realize the data transformations
        
        cassandra.write(ctx, ard.select('chipx', 'chipy', 'ard.dates'), functions.cqlstr('ard.dates'))
        #cassandra.write(ctx, ccd, cqlstr)

        #return {'ard': ard.count(), 'pyccd': ccd.count()}
        print("ard count:{}".format(ard.count()))
        return {'ard': ard.count()}
        
    
    except Exception as e:
        print('error:{}'.format(e))
        traceback.print_exc()
    finally:
        if ctx is not None:
            ctx.stop()
            ctx = None


@click.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
def train(x, y):
    # find a tile for x, y
    # get the tiles neighbor tiles
    # get the chip ids for all 9 tiles
    # get AUX data to cover all 9 tiles
    # filter 
    pass


@click.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
@click.option('--acquired', '-a', required=True)
def classify(x, y, acquired):
    tile = tile(x, y)
    # tile.keys == 'x', 'y', 'extents', 'chips', 'near':{'tiles': [], 'chips': []}'
    pass


@click.group()
def show():
    pass


@show.command()
def models():
    pass


@show.command()
def tile():
    """Display tile status
       
       Returns: {'tileid':
                    {'chipid':
                        'good': 2499,
                        'bad' : 1}}
    """
    pass


if __name__ == '__main__':
    entrypoint()
