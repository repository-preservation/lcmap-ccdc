"""cli.py is the command line interface for CCDC.  

Prerequisites:
1. Set env vars as defined in __init__.py.
2. Pyspark must be available.  This is normally provided by the parent Docker image (lcmap-spark).
3. Chipmunk must be available on the network for ARD and AUX data.
4. Cassandra must be available to read and write pyccd, training, and classification results.

cli.py should be added to setup.py as an entry_point console script.  After installing the CCDC python package, it would then be invoked as the entrypoint of the CCDC Docker image.
"""

from ccdc import ARD
from ccdc import AUX
from ccdc import cassandra
from ccdc import features
from ccdc import grid
from ccdc import ids
from ccdc import logger
from ccdc import metadata
from ccdc import pyccd
from ccdc import randomforest
from ccdc import timeseries

from cytoolz   import do
from cytoolz   import filter
from cytoolz   import first
from cytoolz   import get
from cytoolz   import merge
from cytoolz   import take
from cytoolz   import thread_last
from functools import partial
from merlin    import functions

import ccdc
import click
import datetime
import traceback


def context_settings():
    """Normalized tokens for Click cli

    Returns:
        dict
    """

    return dict(token_normalize_func=lambda x: x.lower())


def acquired():
    """Dynamically generated acquired date range

    Returns:
        str: ISO8601 compliant date range
    """
    
    start = '1776-07-04'
    end   = datetime.datetime.now().isoformat()
    return '{}/{}'.format(start, end)
    

@click.group(context_settings=context_settings())
def entrypoint():
    """Placeholder function to group Click commands"""
    pass


@entrypoint.command()
@click.option('--x',        '-x', required=True)
@click.option('--y',        '-y', required=True)
@click.option('--acquired', '-a', required=False, default=acquired())
@click.option('--number',   '-n', required=False, default=2500)
def changedetection(x, y, acquired=acquired(), number=2500):
    """Run change detection for a tile and save results to Cassandra.
    
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
        ctx  = ccdc.context(name)

        # get logger
        log  = logger(ctx, name)
        
        # wire everything up
        print(ARD)
        tile = grid.tile(x=x, y=y, cfg=ARD)
        cids = ids.rdd(ctx=ctx, cids=list(take(number, tile.get('chips'))))
        ard  = timeseries.rdd(ctx=ctx, cids=cids, acquired=acquired, cfg=ccdc.ARD, name='ard')
        ccd  = pyccd.dataframe(ctx=ctx, rdd=pyccd.rdd(ctx=ctx, timeseries=ard)).cache()

        # emit parameters
        log.info(str(merge(tile, {'acquired': acquired,
                                  'input-partitions': ccdc.INPUT_PARTITIONS,
                                  'product-partitions': ccdc.PRODUCT_PARTITIONS,
                                  'chips': cids.count()})))

        log.info('finding ccd segments...')
        written = pyccd.write(ctx, ccd).count()

        # write metadata
        md =  metadata.detection(ulx=int(get('x', tile)),
                                 uly=int(get('y', tile)),
                                 lrx=int(get('lrx', tile)),
                                 lry=int(get('lry', tile)),
                                 h=int(get('h', tile)),
                                 v=int(get('v', tile)),
                                 acquired=acquired,
                                 detector=pyccd.algorithm(),
                                 ardurl=ccdc.ARD_CHIPMUNK,
                                 segcount=written)
        
        _ = metadata.write(ctx, metadata.dataframe(ctx, md))
                        
        # log and return segment counts
        return do(log.info, "{} complete: {}".format(name, md))
            
    except Exception as e:
        # spark errors & stack trace
        print('{} error:{}'.format(name, e))
        traceback.print_exc()

    finally:
        # stop and/or disconnect Spark
        if ctx is not None:
            ctx.stop()
            ctx = None

            
def training(ctx, cids, acquired=acquired()):
    """Trains and returns a random forest model for the grid

    Args:
        ctx: spark context
        cids: [(x,y), (x1, y1),...]
        acquired: ISO8601 date range "YYYY-MM-DD/YYYY-MM-DD"

    Returns:
        trained model
    """
    log = logger(ctx, __name__)
    model = randomforest.train(ctx, ids.rdd(ctx, cids), acquired)

    if model is None:
        log.warn('Model could not be trained.')
    else:
        log.debug('model type:{}'.format(type(model)))
        log.debug('model:{}'.format(model))

    return model

            
@entrypoint.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
@click.option('--acquired', '-a', required=True)
def classification(x, y, acquired=acquired()): 
    """
    Classify a tile.

    Args:
        acquired (str): ISO8601 date range       
        x (int)       : x coordinate in tile
        y (int)       : y coordinate in tile
        acquired (str): date range of change segments to classify
    """
  
    ctx = None
    name = 'random-forest-classification'
    
    try:
        ctx = ccdc.context(name)
        log = logger(ctx, name)

        log.info('beginning {}...'.format(name))
        log.info('x:{} y:{} acquired:{}'.format(x, y, acquired))

        
        log.info('training model with training grid chip ids...')
        model = training(ctx, grid.training(x, y, AUX), acquired)

        
        log.info('finding classification grid chip ids...')
        cids = ids.dataframe(ctx,
                             ids.rdd(ctx, grid.classification(x, y, AUX)))
        log.info('found {} classification grid chip ids...'.format(cids.count()))
        
        log.info('finding change segments...')
        ccd = pyccd.read(ctx,
                         cids.repartition(ccdc.PRODUCT_PARTITIONS))\
                         .filter('sday >= 0 AND eday >= 0')
         
        log.info('finding aux timeseries...')
        aux = timeseries.aux(ctx,
                             cids.rdd.repartition(ccdc.INPUT_PARTITIONS),
                             acquired).repartition(ccdc.PRODUCT_PARTITIONS)        
       
        log.info('finding classification features...')
        fdf = features.dataframe(aux, ccd)
        
        log.info('predicting classes...')
        preds = randomforest.classify(model, fdf)

        log.info('saving classification results...')
        results = pyccd.join(ccd, preds).persist()

        log.debug('sample result:{}'.format(results.first()))
        
        written = pyccd.write(ctx, randomforest.dedensify(results)).count()

        log.info('saved {} classification results'.format(written))

        return {'x': x, 'y': y, 'acquired': acquired, 'classifications': written}
       
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
