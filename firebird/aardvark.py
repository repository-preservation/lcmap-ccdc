import requests

def inputs():
    return {'red': '',
            'green': '',
            'blue': '',
            'nir': '',
            'swir1', '',
            'swir2', '',
            'thermal', '',
            'cfmask', ''  }

def tile_spec(url):
    """ Returns the tile_spec from aardvark """
    pass


def snap(point, tile_spec):
    """ Snaps a point to a chip id (tile_x, tile_y) """
    pass


def chip_ids(bbox, tile_spec):
    """ Returns all the chip ids (tile_x, tile_y points) needed to 
        cover the bbox """
    pass
