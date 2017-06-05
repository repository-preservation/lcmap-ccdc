from firebird import dates as d
import firebird as fb
import re


def bounds(bounds):
    try:
        for __ in ((ulx, lrx), (lry, uly)):
            if not float(__[0]) < float(__[1]):
                return False
    except:
        return False
    return True


def check_acquired(a):
    if not d.is_acquired(a):
        raise Exception("Acquired dates are invalid: {}".format(a))


def check_bounds(bounds):
    pass


def check_products(products):
    pass


def check_product_dates(product_dates):
    def check(date):
        if not re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', date):
            raise Exception("Invalid product date value: {}".format(date))

    [check(d) for d in product_dates]


def check_clip(clip):
    if not fb.true(clip) or fb.false(clip):
        raise Exception("Clip must be True or False")


def check_chips_fn(chips_fn):
    pass


def check_specs_fn(specs_fn):
    pass


def validate(acquired, bounds, products, product_dates, clip, chips_fn,
             specs_fn):
    check_acquired(acquired)
    check_bounds(bounds)
    check_products(products)
    check_product_dates(product_dates)
    check_clip(clip)
    check_chips_fn(chips_fn)
    check_specs_fn(specs_fn)
