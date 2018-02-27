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
from cytoolz   import take
from cytoolz   import thread_first
from firebird  import ARD
from firebird  import logger
from functools import partial
from merlin.functions import dictionary
from merlin.geometry import extents
from merlin.geometry import coordinates

import firebird
import classification
import click
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
    sc = None
    try:
        name         = 'changedetection'
        sc           = firebird.context(name=name)
        log          = firebird.logger(context=sc, name=name)
        grid         = ARD.get('grid_fn')()
        chip_grid    = first(filter(lambda x: x['name'] == 'chip', grid))
        tile_grid    = first(filter(lambda x: x['name'] == 'tile', grid))
        snap_fn      = ARD.get('snap_fn')
        tilex, tiley = snap_fn(x=x, y=y).get('tile').get('proj-pt')
        tile_extents = extents(ulx=tilex, uly=tiley, grid=tile_grid)
        chips        = coordinates(tile_extents, grid=chip_grid, snap_fn=snap_fn)
            
        log.info('{}'.format(dictionary(name=name,
                                        grid=grid,
                                        snap_fn=snap_fn,
                                        tilex=tilex,
                                        tiley=tiley,
                                        tile_extents=tile_extents,
                                        chips_count=len(chips),
                                        x=x,
                                        y=y,
                                        acquired=acquired)))

        return thread_first(timeseries.ids(sc=sc, chips=take(int(number), chips)),
                            partial(timeseries.execute, sc, acquired=acquired, cfg=ARD),
                            partial(pyccd.execute, sc),
                            partial(pyccd.dataframe, sc),
                            partial(pyccd.write, sc))
    except Exception as e:
        print('error:{}'.format(e))
        traceback.print_exc()
    finally:
        if sc is not None:
            sc.stop()


@click.command()
@click.option('--acquired', '-a', required=True)
@click.option('--point', '-p', required=True)
def train():
    pass


@click.command()
@click.option('--acquired', '-a', required=True)
@click.option('--point', '-p', required=True)
def classify():
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
