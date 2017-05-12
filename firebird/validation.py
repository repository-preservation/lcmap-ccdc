import re
from datetime import date
#from firebird import LCMAP_PRODUCT_DICT


def acquired(ad):
    # 1980-01-01/2015-12-31
    if not re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}\/[0-9]{4}-[0-9]{2}-[0-9]{2}$', ad):
        return False
    return True


def coords(ulx, uly, lrx, lry):
    try:
        for __ in ((ulx, lrx), (uly, lry)):
            if not float(__[0]) < float(__[1]):
                return False
    except:
        return False
    return True


def ord(od):
    try:
        date.fromordinal(int(od))
    except:
        return False
    return True


#def valid_prods(prods):
#    if not set(prods.split(',')) <= set(LCMAP_PRODUCT_DICT):
#        return False
#    return True
