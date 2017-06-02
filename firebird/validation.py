import re




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


def acquired(acquired):
    pass


def bounds(bounds):
    pass


def products(products):
    pass


def product_dates(product_dates):
    pass


def clip(clip):
    pass


def chips_fn(chips_fn):
    pass


def specs_fn(specs_fn):
    pass


def validate(acquired, bounds, products, product_dates, clip, chips_fn,
             specs_fn):
    acquired(acquired)
    bounds(bounds)
    products(products)
    product_dates(product_dates)
    clip(clip)
    chips_fn(chips_fn)
    specs_fn(specs_fn)

    if not acquired(acquired):
        raise Exception("Acquired dates are invalid: {}".format(acquired))

    if not valid.prod(prod_date):
        raise Exception("Invalid product date value: {}".format(prod_date))
