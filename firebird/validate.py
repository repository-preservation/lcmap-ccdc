from firebird import transforms
from merlin import dates as md
from merlin import functions
import re


def is_false(v):
    """Determines if a value is false.

    Args:
        v - a value

    Returns:
        bool: True if v is not None and v is False or 'false' (case insensitive)
    """

    return (v is not None and
            (v == False or (type(v) is str and v.lower() == 'false')))


def is_true(v):
    """Determines if a value is true.

    Args:
        v - a value

    Returns:
        bool: True if v is not None and v is True or 'true' (case insensitive)
    """

    return (v is not None and
            (v == True or (type(v) is str and v.lower() == 'true')))


def _acquired(a):
    """Checks acquired date range as supplied by cmdline
    :param a: Date range string in iso8601 format separated by /
    :return: True if ok, Exception otherwise.
    """
    if not md.is_acquired(a):
        raise Exception("Invalid acquired dates:{}".format(a))
    return True


def _clip(c):
    """Checks clip parameter as supplied by cmdline
    :param c: Clip parameter
    :return: True or Exception
    """
    print("C:{}".format(c))
    print("C TYPE:{}".format(type(c)))

    if is_false(c) or is_true(c):
        return True
    raise Exception("Clip must be true or false")


def _bounds(b):
    """Checks bounds as supplied by cmdline
    :param b: ('123,456', '222,333')
    :return: True or Exception
    """
    if (len(b) > 0 and
        all(map(lambda x: x.find(',') != -1, b)) and
        all(map(lambda x: functions.isnumeric(x),
            functions.flatten(map(lambda i: i.split(','), b))))):
        return True
    raise Exception("Invalid bounds specified:".format(b))


def _products(products):
    """Checks products as supplied by cmdline
    :param products: ('seglength', 'changemag',)
    :return: True or Exception
    """
    available = transforms.labels().keys()
    p = set(products)
    unavailable = p - p.intersection(available)
    if unavailable:
        raise Exception("Invalid product(s):{}".format(unavailable))


def _product_dates(acquired, product_dates):
    """Checks product_dates as supplied by cmdline
    :param acquired: Acquired date range from cmdline
    :param product_dates: ('1980-01-01', '1985-10-1',)
    :return:
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
    """Checks inputs for save() function (via cmdline)
    :param acquired: iso8601 dates separated by /
    :param bounds:
    :param products:
    :param product_dates:
    :param clip:
    :return: True or Exception
    """
    _acquired(acquired)
    _bounds(bounds)
    _clip(clip)
    _products(products)
    _product_dates(product_dates)
    return True
