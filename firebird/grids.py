import firebird
import merlin

def grid():
    """Returns the grid definition associated with Chipmunk ARD"""
    
    return firebird.ARD.get('grid_fn')()


def tile(x, y):
    """Given a point return a tile coordinate
    
       Args:
           x (float): x coordinate
           y (float): y coordinate

       Return:
           dict: {'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}
    """
    # snap
    # parse tile
    # return x,y
    pass


def chips(tile, grid):
    """Given a tile coordinate return a list of chip ids

       Args:
         tile (dict): {'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}
         grid (dict): {}

       Return:
           sequence: [{'ulx', 'uly', 'lrx', 'lry', 'projection-pt', 'grid-pt'}]
    """
    pass


