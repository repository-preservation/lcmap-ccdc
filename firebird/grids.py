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

    return dictionary(x=tilex,
                      y=tiley,
                      h=h,
                      v=v,
                      **tile_extents,
                      chips=chips)


def chips(tile, grid):
    """Given a tile coordinate return a list of chip ids

       Args:
         tile (dict): {'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}
         grid (dict): {}

       Return:
           sequence: [{'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}]
    """
    pass


