from firebird import dates as fd
from firebird import inputs
from firebird import products as fp
from functools import partial
import ccd
import firebird as fb


def simplify_detect_results(results):
    ''' Convert child objects inside CCD results from NamedTuples to dicts '''
    output = dict()
    for key in results.keys():
        output[key] = fb.simplify_objects(results[key])
    return output


def result_to_models(result):
    '''
    Function to extract the change_models dictionary from the CCD results
    :param result: CCD result object (dict)
    :return: list
    '''
    #raise Exception("JSONING:{}".format(result))
    return simplify_detect_results(result)['change_models']


def pyccd(rdd):
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
        results = ccd.detect(dates=data['dates'],
                             blues=data['blues'],
                             greens=data['greens'],
                             reds=data['reds'],
                             nirs=data['nirs'],
                             swir1s=data['swir1s'],
                             swir2s=data['swir2s'],
                             thermals=data['thermals'],
                             quality=data['quality'],
                             params=fb.ccd_params())
        return ((x, y, ccd.algorithm, acquired), results)
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
    return ((x, y, 'lastchange-{}'.format(fp.version), rdd[1]),
            fp.lastchange(result_to_models(data), ord_date=date))


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

    return ((x, y, 'changemag-{}'.format(fp.version), rdd[1]),
            fp.changemag(result_to_models(data), ord_date=date))


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
    return ((x, y, 'changedate-{}'.format(fp.version), rdd[1]),
            fp.changedate(result_to_models(data), ord_date=date))


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
    return ((x, y, 'seglength-{}'.format(fp.version), rdd[1]),
            fp.seglength(result_to_models(data), ord_date=date, bot=bot))


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
    return ((x, y, 'curveqa-{}'.format(fp.version), rdd[1]),
            fp.curveqa(result_to_models(data), ord_date=date))


def fits_in_box(value, bbox):
    '''
    Determines if a point value fits within a bounding box (edges inclusive)
    Useful as a filtering function with conditional enforcement.
    If bbox is None then fits_in_box always returns True

    :param value: Tuple: ((x,y), (data))
    :param bbox: dict with keys: ulx, uly, lrx, lry
    :return: Boolean
    '''
    def fits(point, bbox):
        x,y = point
        return float(x) >= float(bbox['ulx']) and\
               float(x) <= float(bbox['lrx']) and\
               float(y) >= float(bbox['lry']) and\
               float(y) <= float(bbox['uly'])

    return bbox is None or fits(value[0], bbox)


def products(jobconf, sparkcontext):
    """ Product graph for firebird products
    :param jobconf: dict of broadcast variables
    :param sparkcontext: Configured spark context
    :return: dict keyed by product with lazy RDD as value
    """

    jc = jobconf
    sc = sparkcontext

    _chipids = sc.parallelize(jc['chip_ids'].value,
                              jc['initial_partitions'].value)\
                             .setName("CHIP IDS")

    # query data and transform it into pyccd input format
    _in = _chipids.map(partial(inputs.pyccd,
                               specs_url=jc['specs_url'].value,
                               specs_fn=jc['specs_fn'].value,
                               chips_url=jc['chips_url'].value,
                               chips_fn=jc['chips_fn'].value,
                               acquired=jc['acquired'].value,
                               queries=jc['chip_spec_queries'].value))\
                               .flatMap(lambda x: x)\
                               .filter(partial(fits_in_box,
                                               bbox=jc['clip_box'].value))\
                               .map(lambda x: ((x[0][0], x[0][1],
                                                'inputs',
                                                jc['acquired'].value),
                                                x[1]))\
                               .repartition(jc['product_partitions'].value)\
                               .setName('PYCCD INPUTS')

    _ccd = _in.map(pyccd).setName('CCD').persist()

    # cartesian will create an rdd that looks like:
    # (((x, y, algorithm, product_date_str), data), product_date)
    _ccd_dates  = _ccd.cartesian(sc.parallelize(jc['product_dates'].value))

    _lastchange = _ccd_dates.map(lastchange).setName('LASTCHANGE')
    _changemag  = _ccd_dates.map(changemag).setName('CHANGEMAG')
    _changedate = _ccd_dates.map(changedate).setName('CHANGEDATE')
    _seglength  = _ccd_dates.map(seglength).setName('SEGLENGTH')
    _curveqa    = _ccd_dates.map(curveqa).setName('CURVEQA')

    return {'inputs': _in, 'ccd': _ccd, 'lastchange': _lastchange,
            'changemag': _changemag, 'changedate': _changedate,
            'seglength': _seglength, 'curveqa': _curveqa}


def train(product_graph, sparkcontext):
    # training_chipids()
    # requires ancillary data such as DEM, trends, et. al.
    # 
    # TODO: This might require switching to the dataframe api and the
    # spark cassandra connector, especially if we are going to train on results
    # that already exist in cassandra.  Don't implement this without a
    # significant amount of hammock and whiteboard time.
    #
    # In order to send in appropriate chip ids to init, it will have
    # to accept chip ids instead of bounds and the bounds to chip id
    # determination will have to be done by whatever calls it.  This will
    # be necessary as training requires additional areas besides the area
    # one is actually attempting to train on.
    pass


def classify(product_graph, sparkcontext):
    # Same as the training graph.  This cannot run unless
    # #1 - There are ccd results and
    # #2 - The classifier has been trained.
    # Dont just jam these two things into this rdd graph setup.  Find the
    # cleanest way to represent and handle it.  It might require running
    # ccd first, training second and classification third.  Or they might all
    # be able to be put into the same graph and run at the same time.
    #
    # Regardless, all this data will need to be persisted so after its all
    # working we will probably need the ability to load data from iwds,
    # determine what else is needed (what areas are missing based on the
    # request) conditionally produce it, then proceed with the operations
    pass
