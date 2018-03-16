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
from cytoolz   import get
from cytoolz   import merge
from cytoolz   import take
from firebird  import ARD
from firebird  import logger
from functools import partial
from merlin.functions import cqlstr

import cassandra
import click
import firebird
import grids
import pyccd
import timeseries
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
@click.option('--x',        '-x', required=True)
@click.option('--y',        '-y', required=True)
@click.option('--acquired', '-a', required=True)
@click.option('--number',   '-n', required=False, default=2500)
def changedetection(x, y, acquired, number=2500):
    """Run change detection for a tile over a time range and save results to Cassandra.
    
    Args:
        x        (int): tile x coordinate
        y        (int): tile y coordinate
        acquired (str): ISO8601 date range
        number   (int): Number of chips to run change detection on.  Testing only.

    Returns:
        count of saved segments 
    """

    
    ctx  = None
    name = 'change-detection'
    
    try:
        # start and/or connect Spark
        ctx  = firebird.context(name)

        # get logger
        log  = logger(ctx, name)
        
        # wire everything up
        tile = grids.tile(x=x, y=y, cfg=ARD)
        ids  = timeseries.ids(ctx=ctx, chips=take(number, tile.get('chips')))
        ard  = timeseries.rdd(ctx=ctx, ids=ids, acquired=acquired, cfg=firebird.ARD, name='ard')
        ccd  = pyccd.dataframe(ctx=ctx, rdd=pyccd.rdd(ctx=ctx, timeseries=ard)).cache()

        # emit parameters
        log.info(str(merge(tile, {'acquired': acquired,
                                  'input-partitions': firebird.INPUT_PARTITIONS,
                                  'product-partitions': firebird.PRODUCT_PARTITIONS,
                                  'chips': ids.count()})))

        # realize data transformations
        cassandra.write(ctx, ccd, cqlstr(pyccd.algorithm()))

        # log and return segment counts
        return do(log.info "saved {} ccd segments".format(get('ccd', counts)))
            
    except Exception as e:
        # spark errors & stack trace
        print('error:{}'.format(e))
        traceback.print_exc()
        
    finally:
        # stop and/or disconnect Spark
        if ctx is not None:
            ctx.stop()
            ctx = None


@click.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
def train(x, y):

     ctx = None
    try:
        # connect to the cluster
        ctx = firebird.context('random-forest-training')

        # get logger
        log = logger(ctx, 'random-forest-training')

    # make sure tile contains the chip ids for all the neighbor tiles too,
    # or call grids.neighbors(x=x, y=y, cfg=AUX)
    tile = grids.tile(x=x, y=y, cfg=AUX)
    ids  = timeseries.ids(ctx=ctx, chips=take(number, tile.get('chips')))

    # build converter and schema to flatten aux
    aux  = timeseries.dataframe(ctx=ctx,
                                rdd=timeseries.rdd(ctx=ctx,
                                                   ids=ids,
                                                   acquired=acquired,
                                                   cfg=firebird.AUX, name='aux')

    # filter aux for trends != 0 and trends != 9
    # extract chip ids from filtered aux
    # query for matching segments by chip id (pyccd.read or cassandra.read)
    # filter segments on sday > S and eday < E
    # join segments and aux together
    # build features with features UDF, eject unneeded columns
    # train RF
    # save RF
    # return something
                                
    return True


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
