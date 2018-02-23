"""cli.py is the command line interface for Firebird.  

Prerequisites:
1. Set env vars as defined in __init__.py.
2. Pyspark must be available.  This is normally provided by the parent Docker image (lcmap-spark).
3. Chipmunk must be available on the network for ARD and AUX data.
4. Cassandra must be available to read and write pyccd, training, and classification results.

cli.py should be added to setup.py as an entry_point console script.  After installing the Firebird python package, it would then be invoked as the entrypoint of the Firebird Docker image.
"""

from cytoolz  import excepts
from cytoolz  import partial
from cytoolz  import thread_first
from firebird import ARD
from firebird import logger
from merlin.geometry import extents
from merlin.geometry import coordinates

import classification
import click
import pyccd
import training


def dictionary(**kwargs):
    """Converts kwargs to a dictionary."""
    return kwargs


@click.command()
@click.option('--x', required=True)
@click.option('--y', required=True)
@click.option('--acquired', required=True)
def changedetection(x, y, acquired):
    """Run change detection for a tile over a time range and save results to Cassandra.
    
    Args:
        x (int): tile x coordinate
        y (int): tile y coordinate
        acquired (str): ISO8601 date range

    Returns:
        TBD

    """

    name         = 'changedetection'
    grid         = ARD.get('grid_fn')()
    snap_fn      = ARD.get('snap_fn')
    tilex, tiley = snap_fn(x=x, y=y).get('tile').get('proj-pt')
    tile_extents = extents(ulx=tilex, uly=tiley, grid=tg)
    chips        = coordinates(tile_extents, grid=grid.get('chips'), snap_fn=snap_fn)
    sc           = firebird.context(name=name)
    log          = firebird.logger(context=sc, name=name)
    
    try:    
        log.info('{}{}'.format(tilex, tiley))
    
        log.debug('{}'.format(dictionary(name=name,
                                         grid=grid,
                                         snap_fn=snap_fn,
                                         tilex=tilex,
                                         tiley=tiley,
                                         tile_extents=tile_extents,
                                         chips=chips,
                                         x=x,
                                         y=y,
                                         acquired=acquired)))

        return thread_first(timeseries.execute(sc=sc, chips=chips, acquired=acquired),
                            partial(pyccd.execute, sc=sc),
                            partial(pyccd.dataframe, sc=sc),
                            partial(pyccd.write, sc=sc))
    except Exception as e:
        log.error('error:{}'.format(e))
    finally:
        excepts(Exception, sc.stop(), lambda _: None)()


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


@show.command()
def tile():
    """Display tile status
       
       Returns: {'tileid':
                    {'chipid':
                        'good': 2499,
                        'bad' : 1}}
    """
    pass


@show.command()
def models():
    pass


@click.group()
def show():
    pass

