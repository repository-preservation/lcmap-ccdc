import re


def acquired(ad):
    # 1980-01-01/2015-12-31
    if not re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}\/[0-9]{4}-[0-9]{2}-[0-9]{2}$', ad):
        return False
    return True


def bounds(bounds):
    try:
        for __ in ((ulx, lrx), (lry, uly)):
            if not float(__[0]) < float(__[1]):
                return False
    except:
        return False
    return True


def prod(pd):
    # 1980-01-01
    if not re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', pd):
        return False
    return True
