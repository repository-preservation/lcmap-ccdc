from firebird import transforms
from merlin import dates as md
from merlin import functions
import re

def is_false(v):
    """Determines if a value is false.

    Args:
        v: a value

    Returns:
        bool: True if v is not None and v is False or 'false' (case insensitive)
    """

    return (v is not None and
            (v == False or (type(v) is str and v.lower() == 'false')))


def is_true(v):
    """Determines if a value is true.

    Args:
        v: a value

    Returns:
        bool: True if v is not None and v is True or 'true' (case insensitive)
    """

    return (v is not None and
            (v == True or (type(v) is str and v.lower() == 'true')))


def _acquired(a):
    """Checks acquired date range as supplied by cmdline

    Args:
        a (str): iso8601 date range

    Returns:
        bool: True if ok, Exception otherwise.
    """

    if not md.is_acquired(a):
        raise Exception("Invalid acquired dates:{}".format(a))
    return True


def _clip(c):
    """Checks clip parameter as supplied by cmdline

    Args:
        c (str): Clip parameter

    Returns:
        bool: True or Exception
    """

    if is_false(c) or is_true(c):
        return True
    raise Exception("Clip must be true or false")


def _bounds(b):
    """Checks bounds as supplied by cmdline

    Args:
        b (tuple): ('123,456', '222,333')

    Returns:
        bool: True or Exception
    """

    if (len(b) > 0 and
        all(map(lambda x: x.find(',') != -1, b)) and
        all(map(lambda x: functions.isnumeric(x),
            functions.flatten(map(lambda i: i.split(','), b))))):
        return True
    raise Exception("Invalid bounds specified:".format(b))


def _products(products):
    """Checks products as supplied by cmdline

    Args:
        products (tuple): ('seglength', 'changemag',)

    Returns:
        True or Exception
    """

    available = transforms.labels().keys()
    p = set(products)
    unavailable = p - p.intersection(available)
    if unavailable:
        raise Exception("Invalid product(s):{}".format(unavailable))


def _product_dates(acquired, product_dates):
    """Checks product_dates as supplied by cmdline

    Args:
        acquired (str): iso8601 date range
        product_dates (tuple): ('1980-01-01', '1985-10-1',)

    Returns:
        bool: True or Exception
    """

    def check(date):
        if not re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', date):
            raise Exception("Invalid product date value: {}".format(date))
        start = md.to_ordinal(md.startdate(acquired))
        end = md.to_ordinal(md.enddate(acquired))
        val = md.to_ordinal(date)
        if val < start or val > end:
            raise Exception("Product date {} out of range{}".format(date,
                                                                    acquired))

    return [check(d) for d in product_dates]


def save(acquired, bounds, products, product_dates, clip):
    """Checks save() function parameters

    Args:
        acquired (str): iso8601 date range
        bounds (tuple): tuple of tuple.  1 to N.  ((x1, y1), (x1, y2),)
        products (tuple): ('product1', 'product2', 'product3',)
        product_dates (tuple): sequence of iso8601 dates
        clip (str): True or False

    Returns:
        True or Exception
    """

    _acquired(acquired)
    _bounds(bounds)
    _clip(clip)
    _products(products)
    _product_dates(acquired=acquired, product_dates=product_dates)
    return True
