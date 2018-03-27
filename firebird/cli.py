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
import features
import firebird
import grid
import ids
import pyccd
import randomforest
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
        cids = ids.rdd(ctx=ctx, chips=take(number, tile.get('chips')))
        ard  = timeseries.rdd(ctx=ctx, cids=cids, acquired=acquired, cfg=firebird.ARD, name='ard')
        ccd  = pyccd.dataframe(ctx=ctx, rdd=pyccd.rdd(ctx=ctx, timeseries=ard)).cache()

        # emit parameters
        log.info(str(merge(tile, {'acquired': acquired,
                                  'input-partitions': firebird.INPUT_PARTITIONS,
                                  'product-partitions': firebird.PRODUCT_PARTITIONS,
                                  'chips': cids.count()})))

        written = pyccd.write(ctx, ccd).count()
        
        # log and return segment counts
        return do(log.info, "saved {} ccd segments".format(written))
            
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
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
@click.option('--acquired', '-a', required=True)
def classify(x, y, acquired):
    """
    Args:
        acquired (str): ISO8601 date range       
        x (int)       : x coordinate in tile
        y (int)       : y coordinate in tile
    """
  
    ctx = None
    name = 'random-forest-classification'
    
    try:
        ctx   = firebird.context(name)

        # get logger
        log   = logger(ctx, name)
        
        # train a model
        tids  = ids.rdd(ctx, grid.training(x=x, y=y, cfg=AUX))
        tile  = grid.tile(x=x, y=y, cfg=AUX)
        model = randomforest.train(ctx, tids, acquired)

        if model is None:
            log.warn('Model could not be trained... exiting')
            return 1

        # pull data to classify
        cids  = ids.dataframe(ctx, ids.rdd(ctx, get('chips', tile)))
        ccd   = pyccd.read(ctx, cids).filter('sday >= 0 AND eday >= 0')
        aux   = timeseries.aux(ctx, cids.rdd, acquired)

        # classify it
        fdf   = features.dataframe(aux, ccd)
        preds = randomforest.classify(model, fdf)
        log.info('Preds:{}'.format(preds))
        log.info('Sample prediction:{}'.format(preds.first()))
        
        # join class onto ARD dataframe
        # save
        # done

        #log.debug('training chip count:{}'.format(ids.count()))
        #log.debug('aux point count:{}'.format(aux.count()))
        #log.debug('aux chip count:{}'.format(cid.count()))
        #log.debug('feature point count:{}'.format(fdf.count()))
        #log.debug('sample feature:{}'.format(fdf.first()))
        #log.debug('sample model:{}'.format(model.predictionCol))
        

    except Exception as e:
        # spark errors & stack trace
        print('{} error:{}'.format(name, e))
        traceback.print_exc()    
    finally:
        # stop and/or disconnect Spark
        if ctx is not None:
            ctx.stop()
            ctx = None


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
