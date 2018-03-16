from cytoolz import first
from merlin.geometry import extents
from merlin.geometry import coordinates

import firebird
import merlin

def grid():
    """Returns the grid definition associated with Chipmunk ARD"""
    
    return firebird.ARD.get('grid_fn')()


        #grid         = ARD.get('grid_fn')()
        #chip_grid    = first(filter(lambda x: x['name'] == 'chip', grid))
        #tile_grid    = first(filter(lambda x: x['name'] == 'tile', grid))
        #snap_fn      = ARD.get('snap_fn')
        #tilex, tiley = snap_fn(x=x, y=y).get('tile').get('proj-pt')
        #tile_extents = extents(ulx=tilex, uly=tiley, grid=tile_grid)
        #chips        = take(int(number),
        #                    coordinates(tile_extents, grid=chip_grid, snap_fn=snap_fn))

        # END move into function(s)


def tile(x, y, cfg):
    """Given a point return a tile
    
       Args:
           x (float): x coordinate
           y (float): y coordinate

       Return:
           dict: {'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}
    """
    grid = cfg.get('grid_fn')()
    chip_grid = first(filter(lambda x: x['name'] == 'chip', grid))
    tile_grid = first(filter(lambda x: x['name'] == 'tile', grid))

    snap_fn = cfg.get('snap_fn')
    snapped = snap_fn(x=x, y=y)
    tilex, tiley = snapped.get('tile').get('proj-pt')
    h, v = snapped.get('tile').get('grid-pt')
    #near = near_fn(x=x, y=y)
    tile_extents = extents(ulx=tilex, uly=tiley, grid=tile_grid)
    chips = coordinates(tile_extents, grid=chip_grid, snap_fn=snap_fn)

    return dict(x=tilex,
                y=tiley,
                h=h,
                v=v,
                **tile_extents,
                chips=chips)


def training(x, y, cfg):
    """Returns the chip ids for training"""

    grid    = cfg.get('grid_fn')()
    snap_fn = cfg.get('snap_fn')()
    near_fn = cfg.get('near_fn')()

    return chips
