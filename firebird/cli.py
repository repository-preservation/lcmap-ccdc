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
from firebird  import AUX
from firebird  import logger
from functools import partial

import cassandra
import click
import firebird
import grid
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
        tile = grid.tile(x=x, y=y, cfg=ARD)
        ids  = timeseries.ids(ctx=ctx, chips=take(number, tile.get('chips')))
        ard  = timeseries.rdd(ctx=ctx, ids=ids, acquired=acquired, cfg=firebird.ARD, name='ard')
        ccd  = pyccd.dataframe(ctx=ctx, rdd=pyccd.rdd(ctx=ctx, timeseries=ard)).cache()

        # emit parameters
        log.info(str(merge(tile, {'acquired': acquired,
                                  'input-partitions': firebird.INPUT_PARTITIONS,
                                  'product-partitions': firebird.PRODUCT_PARTITIONS,
                                  'chips': ids.count()})))

        # realize data transformations
        cassandra.write(ctx, ccd, pyccd.table())

        # log and return segment counts
        return do(log.info, "saved {} ccd segments".format(ccd.count()))
            
    except Exception as e:
        # spark errors & stack trace
        print('{} error:{}'.format(name, e))
        traceback.print_exc()

    finally:
        # stop and/or disconnect Spark
        if ctx is not None:
            ctx.stop()
            ctx = None


@entrypoint.command()
@click.option('--x',        '-x', required=True)
@click.option('--y',        '-y', required=True)
@click.option('--acquired', '-a', required=True)
@click.option('--number',   '-n', required=False, default=22500)
def train(x, y, acquired, number=22500):
    """Trains and saves random forest model for a tile

    Args:
        x (int)       : x coordinate in tile
        y (int)       : y coordinate in tile
        acquired (str): ISO8601 date range 
        number (int)  : Number of chips as training data. Testing only.

    Returns:
        TBD
    """
    
    ctx  = None
    name = 'random-forest-training'
     
    try:
        # start and/or connect Spark
        ctx = firebird.context(name)

        # get logger
        log = logger(ctx, name)

        # wire everything up
        # parameterize the grid being passed in so you can test
        # this it the ops grid
        # chips = grid.training(x=x, y=y, cfg=AUX)

        chips = get('chips', grid.tile(x=x, y=y, cfg=AUX))
        
        ids   = timeseries.ids(ctx=ctx, chips=take(number, chips)).persist()
                
        # get aux dataframe
        aux   = timeseries.aux(ctx=ctx, ids=ids, acquired=acquired)\
                          .filter('trends[0] NOT IN (1, 8)')\
                          .persist()

        # get chip ids to query
        cid   = aux.select(aux.chipx, aux.chipy).distinct().persist()
                
        # Pull results for each chip
        ccd   = cid.join(cassandra.read(ctx, pyccd.table()),
                         on=['chipx', 'chipy'],
                         how='inner').filter('sday >= 0 AND eday <= 0').persist()

        # merge aux data with ccd results
        both  = aux.join(ccd, on=['chipx', 'chipy', 'x', 'y'], how='inner').persist()

        log.debug('training chip count:{}'.format(ids.count()))
        log.debug('aux point count:{}'.format(aux.count()))
        log.debug('aux chip count:{}'.format(cid.count()))
        log.debug('training point count:{}'.format(both.count()))
        log.debug('sample training point:{}'.format(both.first()))
        
        # build features with features UDF, eject unneeded columns
        # train RF
        # save RF
        # return something

                
    except Exception as e:
        # spark errors & stack trace
        print('{} error:{}'.format(name, e))
        traceback.print_exc()
        
    finally:
        # stop and/or disconnect Spark
        if ctx is not None:
            ctx.stop()
            ctx = None
                                
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
