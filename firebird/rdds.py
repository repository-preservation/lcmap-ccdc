from firebird import dates as fd
from firebird import products
import ccd as pyccd
import firebird as fb


def ccd(rdd):
    '''
    Execute ccd.detect
    :param rdd: Tuple of (tuple, dict) generated from pyccd_inputs
                ((x, y, algorithm, datestring): data)
    :return: A tuple of (tuple, dict) with pyccd results
             ((x, y, algorithm, acquired), results)
    '''
    x = rdd[0][0]
    y = rdd[0][1]
    acquired = rdd[0][3]
    data = rdd[1]
    try:
        results = pyccd.detect(dates=data['dates'],
                               blues=data['blues'],
                               greens=data['greens'],
                               reds=data['reds'],
                               nirs=data['nirs'],
                               swir1s=data['swir1s'],
                               swir2s=data['swir2s'],
                               thermals=data['thermals'],
                               quality=data['quality'],
                               params=fb.ccd_params())
        return ((x, y, pyccd.algorithm, acquired), results)
    except Exception as e:
        fb.logger.error("Exception running ccd.detect: {}".format(e))


def lastchange(rdd):
    '''
    Create lastchange product
    :param rdd: (((x, y, algorithm, acquired), data), product_date)
    :return: ((x, y, algorithm, result))
    '''
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    date = fd.to_ordinal(rdd[1])
    return ((x, y, 'lastchange-{}'.format(products.version), rdd[1]),
            products.lastchange(data, ord_date=date))


def changemag(rdd):
    '''
    Create changemag product
    :param rdd: (((x, y, algorithm, acquired), data), product_date)
    :return: ((x, y, algorithm, result))
    '''
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    date = fd.to_ordinal(rdd[1])

    return ((x, y, 'changemag-{}'.format(products.version), rdd[1]),
            products.changemag(data, ord_date=date))


def changedate(rdd):
    '''
    Create changedate product
    :param rdd: (((x, y, algorithm, acquired), data), product_date)
    :return: ((x, y, algorithm, result))
    '''
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    date = fd.to_ordinal(rdd[1])
    return ((x, y, 'changedate-{}'.format(products.version), rdd[1]),
            products.changedate(data, ord_date=date))


def seglength(rdd):
    '''
    Create seglength product
    :param rdd: (((x, y, algorithm, acquired), data), product_date)
    :return: ((x, y, algorithm, result))
    '''
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    date = fd.to_ordinal(rdd[1])
    bot = fd.to_ordinal(fd.startdate(rdd[0][0][3]))
    return ((x, y, 'seglength-{}'.format(products.version), rdd[1]),
            products.seglength(data, ord_date=date, bot=bot))


def curveqa(rdd):
    '''
    Create curveqa product
    :param rdd: (((x, y, algorithm, acquired), data), product_date)
    :return: ((x, y, algorithm, result))
    '''
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    date = fd.to_ordinal(rdd[1])
    return ((x, y, 'curveqa-{}'.format(products.version)}, rdd[1]),
            products.curveqa(data, ord_date=date))