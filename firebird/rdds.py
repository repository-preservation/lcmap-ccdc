from firebird import ccd_params
from firebird import dates as fd
from firebird import functions as f
from firebird import inputs
from firebird import logger
from firebird import products as fp
from functools import partial
from functools import wraps
import ccd


def algorithm(name, version):
    """Standardizes algorithm name and version representation.
    :param name: Name of algorithm
    :param version: Version of algorithm
    :return: name_version
    """
    return '{}_{}'.format(name, version)


def success(x, y, alg, datestr, result):
    """Formats an rdd transformation result.
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: datestr that identifies the result
    :param result: algorithm outputs
    :return: ((x, y, alg, datestr), result, None)
    """
    return ((x, y, alg, datestr), result, None)


def error(x, y, alg, datestr, errors):
    """Format an rdd transformation error
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: datestr that identifies the result
    :param errors: algorithm errors
    :return: ((x, y, alg, datestr), None, errors)
    """
    return ((x, y, alg, datestr), None, errors)


def haserrors(x, y, alg, datestr, errors):
    """Determines if previous errors exist and creates proper return value
    if True.  If no error exists returns False.
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: datestr for current RDD record
    :param errs: Errors element from input RDD
    :return: Either a properly formatted RDD tuple or the result of executing
             the RDD function.
    """
    if errors is None:
        return False
    else:
        e = 'previous-error:{}'.format(errors)
        return error(x=x, y=y, alg=alg, datestr=datestr, errors=e)


def tryexcept(func, kwargs, x, y, alg, datestr):
    """Executes a function wrapped in try: except:.  Returns result
    of success() or error().
    :param func: function to execute
    :param kwargs: keyword args for func
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: date string that identifies this execution
    :return: value of success() or error()
    """
    try:
        return success(x=x, y=y, alg=alg, datestr=datestr,
                       result=func(**kwargs))
    except Exception as errs:
        return error(x=x, y=y, alg=alg, datestr=datestr, errors=errs)


def safely(func, kwargs, x, y, alg, datestr, errors):
    """Runs a function for an input with exception handling applied
    :param func: function to execute
    :param kwargs: keyword args for func
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: date string that identifies this execution
    :param errors: value of input rdd tuple position for errors.
    :return: value of success() or error()
    """
    return (haserrors(x=x, y=y, alg=alg, datestr=datestr, errors=errors) or
            tryexcept(func=func, kwargs=kwargs, x=x, y=y, alg=alg,
                      datestr=datestr))


def simplify_detect_results(results):
    """Convert child objects inside CCD results from NamedTuples to dicts"""

    def simplify(result):
        return {k: f.simplify_objects(v) for k, v in result.items()}

    return simplify(results) if type(results) is dict else dict()


def result_to_models(result):
    """Function to extract the change_models dictionary from the CCD results
    :param result: CCD result object (dict)
    :return: list
    """
    return simplify_detect_results(result).get('change_models')


def pyccd(rdd):
    """Execute ccd.detect
    :param rdd: Tuple of (tuple, dict) generated from pyccd_inputs
                ((x, y, algorithm, datestring): data)
    :return: A tuple of (tuple, dict) with pyccd results
             ((x, y, algorithm, acquired), results, errors)
    """
    x = rdd[0][0]
    y = rdd[0][1]
    acquired = rdd[0][3]
    data = rdd[1] or dict()
    errs = rdd[2]
    kwargs = {'dates': data.get('dates'),
              'blues': data.get('blues'),
              'greens': data.get('greens'),
              'reds': data.get('reds'),
              'nirs': data.get('nirs'),
              'swir1s': data.get('swir1s'),
              'swir2s': data.get('swir2s'),
              'thermals': data.get('thermals'),
              'quality': data.get('quality'),
              'param': ccd_params()}

    return safely(func=ccd.detect, kwargs=kwargs, x=x, y=y, alg=ccd.algorithm,
                  datestr=acquired, errors=errs)


def lastchange(rdd):
    """Create lastchange product
    :param rdd: (((x, y, algorithm, acquired), data, errors), product_date)
    :return: ((x, y, algorithm, result, errors))
    """
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data), 'ord_date': fd.to_ordinal(date)}

    return safely(func=fp.lastchange, kwargs=kwargs, x=x, y=y,
                  alg=algorithm('lastchange', fp.version), datestr=date,
                  errors=errs)


def changemag(rdd):
    """Create changemag product
    :param rdd: (((x, y, algorithm, acquired), data, errors), product_date)
    :return: ((x, y, algorithm, result))
    """
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data), 'ord_date': fd.to_ordinal(date)}

    return safely(func=fp.changemag, kwargs=kwargs, x=x, y=y,
                  alg=algorithm('changemag', fp.version), datestr=date,
                  errors=errs)


def changedate(rdd):
    """Create changedate product
    :param rdd: (((x, y, algorithm, acquired), data, errors), product_date)
    :return: ((x, y, algorithm, result))
    """
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data), 'ord_date': fd.to_ordinal(date)}

    return safely(func=fp.changedate, kwargs=kwargs, x=x, y=y,
                  alg=algorithm('changedate', fp.version), datestr=date,
                  errors=errs)


def seglength(rdd):
    """Create seglength product
    :param rdd: (((x, y, algorithm, acquired), data, errors), product_date)
    :return: ((x, y, algorithm, result))
    """
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    acquired = rdd[0][0][3]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data),
              'ord_date': fd.to_ordinal(date),
              'bot': fd.to_ordinal(fd.startdate(acquired))}

    return safely(func=fp.seglength, kwargs=kwargs, x=x, y=y,
                  alg=algorithm('seglength', fp.version), datestr=date,
                  errors=errs)


def curveqa(rdd):
    """Create curveqa product
    :param rdd: (((x, y, algorithm, acquired), data, errors), product_date)
    :return: ((x, y, algorithm, result))
    """
    x = rdd[0][0][0]
    y = rdd[0][0][1]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data), 'ord_date': fd.to_ordinal(date)}

    return safely(func=fp.curveqa, kwargs=kwargs, x=x, y=y,
                  alg=algorithm('curveqa', fp.version), datestr=date,
                  errors=errs)


def fits_in_box(value, bbox):
    """Determines if a point value fits within a bounding box (edges inclusive)
    Useful as a filtering function with conditional enforcement.
    If bbox is None then fits_in_box always returns True

    :param value: Tuple: ((x,y), (data))
    :param bbox: dict with keys: ulx, uly, lrx, lry
    :return: Boolean
    """
    def fits(point, bbox):
        x, y = point
        return (float(x) >= float(bbox['ulx']) and
                float(x) <= float(bbox['lrx']) and
                float(y) >= float(bbox['lry']) and
                float(y) <= float(bbox['uly']))

    return bbox is None or fits(value[0], bbox)


def labels(inputs=None, ccd=None, lastchange=None, changemag=None,
           changedate=None, seglength=None, curveqa=None):
    """Applies friendly names to products
    :param inputs: Inputs rdd
    :param ccd: CCD rdd
    :param lastchange: Lastchange rdd
    :param changemag: Changemag rdd
    :param changedate: Changedate rdd
    :param seglength: Seglength rdd
    :param curvaqa: Curveqa rdd
    :return: A dict of label:rdd
    """
    return {'inputs': inputs, 'ccd': ccd, 'lastchange': lastchange,
            'changemag': changemag, 'changedate': changedate,
            'seglength': seglength, 'curveqa': curveqa}


def products(jobconf, sparkcontext):
    """Product graph for firebird products
    :param jobconf: dict of broadcast variables
    :param sparkcontext: Configured spark context or None
    :return: dict keyed by product with lazy RDD as value
    """

    jc = jobconf
    sc = sparkcontext

    acquired = jc['acquired'].value
    specs_url = jc['specs_url'].value
    specs_fn = jc['specs_fn'].value
    chips_url=jc['chips_url'].value
    chips_fn=jc['chips_fn'].value
    queries = jc['chip_spec_queries'].value
    clip_box = jc['clip_box'].value
    initial_partitions = jc['initial_partitions'].value
    product_partitions = jc['product_partitions'].value
    chip_ids = jc['chip_ids'].value

    _chipids = sc.parallelize(chip_ids, initial_partitions).setName("chip_ids")

    # query data and transform it into pyccd input format
    _in = _chipids.map(partial(inputs.pyccd,
                               specs_url=specs_url,
                               specs_fn=specs_fn,
                               chips_url=chips_url,
                               chips_fn=chips_fn,
                               acquired=acquired,
                               queries=queries))\
                               .flatMap(lambda x: x)\
                               .filter(partial(fits_in_box,
                                               bbox=clip_box))\
                               .map(lambda x: (success(x=x[0][0],
                                                       y=x[0][1],
                                                       alg='inputs',
                                                       datestr=acquired,
                                                       result=x[1])))\
                               .repartition(product_partitions)\
                               .setName('pyccd_inputs')

    _ccd = _in.map(pyccd).setName(ccd.algorithm).persist()

    # cartesian will create an rdd that looks like:
    # (((x, y, algorithm, product_date_str), data), product_date)
    _ccd_dates = _ccd.cartesian(sc.parallelize(jc['product_dates'].value))

    _lc = _ccd_dates.map(lastchange).setName('lastchange_v1')
    _cm = _ccd_dates.map(changemag).setName('changemag_v1')
    _cd = _ccd_dates.map(changedate).setName('changedate_v1')
    _sl = _ccd_dates.map(seglength).setName('seglength_v1')
    _qa = _ccd_dates.map(curveqa).setName('curveqa_v1')

    return labels(inputs=_in, ccd=_ccd, lastchange=_lc, changemag=_cm,
                  changedate=_cd, seglength=_sl, curveqa=_qa)


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
