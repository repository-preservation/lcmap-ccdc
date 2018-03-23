from cytoolz  import first
from cytoolz  import get
from cytoolz  import get_in
from cytoolz  import second
from cytoolz  import thread_last
from functools import partial
from merlin.functions import flatten
from merlin.geometry import extents
from merlin.geometry import coordinates
from operator import eq

import firebird
import merlin


def definition(cfg=firebird.ARD):
    """Returns the grid definition associated with configuration"""
    
    return get('grid_fn', cfg)()


def tile(x, y, cfg):
    """Given a point return a tile
    
    Args:
        x (float): x coordinate
        y (float): y coordinate

    Return:
        dict: {'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}
    """

    # get all grid definitions
    grid    = definition()

    # get tile & chip grids
    tgrid   = first(filter(lambda x: eq(get('name', x), 'tile'), grid))
    cgrid   = first(filter(lambda x: eq(get('name', x), 'chip'), grid))

    snap_fn = get('snap_fn', cfg)
    snapped = snap_fn(x=x, y=y)
    tx, ty  = get_in(['tile', 'proj-pt'], snapped)
    h, v    = get_in(['tile', 'grid-pt'], snapped)
    exts    = extents(ulx=tx, uly=ty, grid=tgrid)
    chips   = coordinates(exts, grid=cgrid, snap_fn=snap_fn)

    return dict(x=tx,
                y=ty,
                h=h,
                v=v,
                **exts,
                chips=chips)


def training(x, y, cfg):
    """Returns the chip ids for training
    
    Args:
        x   (int):  x coordinate in tile 
        y   (int):  y coordinate in tile
        cfg (dict): a Merlin configuration

    Returns:
        list of chip ids for training area
    """

    near_fn = get('near_fn', cfg)

    return thread_last(near_fn(x=x, y=y),
                       partial(get, 'tile'),
                       partial(map, lambda t:  t.get('proj-pt')),
                       partial(map, lambda xy: tile(x=first(xy), y=second(xy), cfg=cfg)),
                       partial(map, lambda t:  get('chips', t)),
                       flatten,
                       list)

   

