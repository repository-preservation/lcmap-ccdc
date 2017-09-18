from firebird import ccd_params
from firebird import products as fp
from functools import partial
from functools import wraps
from merlin import chips
from merlin import dates
from merlin import functions as f
from merlin import timeseries
import ccd
import merlin

# dataframes from dict rdds
# ss.createDataFrame(rdd, ['chip_x', 'chip_y', 'x', 'y'])


def algorithm(name, version):
    """Standardizes algorithm name and version representation.
    :param name: Name of algorithm
    :param version: Version of algorithm
    :return: name_version
    """
    return '{}_{}'.format(name, version)


def result(chip_x, chip_y, x, y, alg, datestr):
    """Formats an rdd transformation result.
    :param chip_x: x coordinate of source chip id
    :param chip_y: y coordinate of source chip id
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: datestr that identifies the result
    :param data: algorithm outputs
    :return: {chip_x, chip_y, x, y, alg, datestr, data}
    """
    return {'chip_x': chip_x,
            'chip_y': chip_y,
            'x': x,
            'y': y,
            'alg': alg,
            'datestr': datestr}


def success(chip_x, chip_y, x, y, alg, datestr, data):
    return merge(result(chip_x, chip_y, x, y, alg, datestr), {'result': data})


def error(chip_x, chip_y, x, y, alg, datestr, data):
    return merge(result(chip_x, chip_y, x, y, alg, datestr), {'error': data})


def haserrors(result):
    """Determines if previous errors exist and creates proper return value
    if True.  If no error exists returns False.
    :param chip_x: x coordinate of source chip id
    :param chip_y: y coordinate of source chip id
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: datestr for current RDD record
    :param errs: Errors element from input RDD
    :return: Either a properly formatted RDD tuple or the result of executing
             the RDD function.
    """
    if 'error' in result:
        msg = 'upstream->{}:{}'
        return merge(result, {'error': msg.format(alg, get('error', result))})
    else:
        return False


def tryexcept(func, kwargs, chip_x, chip_y, x, y, alg, datestr):
    """Executes a function wrapped in try: except:.  Returns result
    of success() or error().
    :param func: function to execute
    :param kwargs: keyword args for func
    :param chip_x: x coordinate of source chip id
    :param chip_y: y coordinate of source chip id
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: date string that identifies this execution
    :return: value of success() or error()
    """
    try:
        return success(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                       datestr=datestr, data=func(**kwargs))
    except Exception as errs:
        return error(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                     datestr=datestr, data=errs)


def safely(func, kwargs, chip_x, chip_y, x, y, alg, datestr, errors):
    """Runs a function for an input with exception handling applied
    :param func: function to execute
    :param kwargs: keyword args for func
    :param chip_x: x coordinate of source chip id
    :param chip_y: y coordinate of source chip id
    :param x: x coordinate
    :param y: y coordinate
    :param alg: algorithm and version string
    :param datestr: date string that identifies this execution
    :param errors: value of input rdd tuple position for errors.
    :return: value of success() or error()
    """
    return (haserrors(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                      datestr=datestr, errors=errors) or
            tryexcept(func=func, kwargs=kwargs, chip_x=chip_x, chip_y=chip_y,
                      x=x, y=y, alg=alg, datestr=datestr))


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
    :param rdd: dict of chip_x, chip_y, x, y, alg, datestr, results, errors
                generated from pyccd_inputs
    :return: dict of pyccd results
             {chip_x, chip_y, x, y, algorithm, acquired, results, errors}
    """

    return safely(func=ccd.detect,
                  kwargs=merge(get(rdd, 'result', {}),
                               {'params': ccd_params()}),
                  chip_x=rdd['chip_x'],
                  chip_y=rdd['chip_y'],
                  x=rdd['x'],
                  y=rdd['y'],
                  alg=ccd.algorithm,
                  datestr=rdd['acquired'],
                  errors=rdd['errors'])


def fits_in_box(value, bbox):
    """Determines if a point value fits within a bounding box (edges inclusive)
    Useful as a filtering function with conditional enforcement.
    If bbox is None then fits_in_box always returns True

    :param value: Tuple: ((chip_x, chip_y, x, y), (data))
    :param bbox: dict with keys: ulx, uly, lrx, lry
    :return: Boolean
    """
    def fits(point, bbox):
        _, _, x, y = point
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

    sc = sparkcontext

    acquired = jobconf['acquired'].value
    specs_fn = jobconf['specs_fn'].value
    chips_url = jobconf['chips_url'].value
    chips_fn = jobconf['chips_fn'].value
    queries = jobconf['chip_spec_queries'].value
    clip_box = jobconf['clip_box'].value
    initial_partitions = jobconf['initial_partitions'].value
    product_partitions = jobconf['product_partitions'].value
    chip_ids = jobconf['chip_ids'].value

    _chipids = sc.parallelize(chip_ids, initial_partitions).setName("chip_ids")

    # query data and transform it into pyccd input format

    _in = _chipids.map(partial(merlin.create,
                               dates_fn=partial(
                                           f.chexists,
                                           check_fn=timeseries.symmetric_dates,
                                           keys=['quality']),
                               specs_fn=specs_fn,
                               chips_url=chips_url,
                               chips_fn=chips_fn,
                               acquired=acquired,
                               queries=queries))\
                               .flatMap(lambda x: x)\
                               .filter(partial(fits_in_box,
                                               bbox=clip_box))\
                               .map(lambda x: (success(chip_x=x[0][0],
                                                       chip_y=x[0][1],
                                                       x=x[0][2],
                                                       y=x[0][3],
                                                       alg=algorithm('inputs',
                                                                     '20170608'),
                                                       datestr=acquired,
                                                       result=x[1])))\
                               .repartition(product_partitions)\
                               .setName(algorithm('inputs', '20170608'))



    return labels(inputs=_in, ccd=_ccd, lastchange=_lc, changemag=_cm,
                  changedate=_cd, seglength=_sl, curveqa=_qa)


def chipids(jobconf, sparkcontext):
    return sparkcontext.parallelize(
            jobconf['chip_ids'].value,
            jobconf['initial_partitions'].value).setName('chipids')


def timeseries(rdd, jobconf, sparkcontext):
    return rdd.map(partial(merlin.create,
                           dates_fn=jobconf['dates_fn'].value,
                           specs_fn=jobconf['specs_fn'].value,
                           chips_url=jobconf['chips_url'].value,
                           chips_fn=jobconf['chips_fn'].value,
                           acquired=jobconf['acquired'].value,
                           queries=jobconf['queries'].value))\
                           .setName(algorithm('merlin', '1.0'))


def changemodels(rdd, jobconf, sparkcontext):
    df = read.cassandra(ccd.algorithm)

    existing = changemodels.get(jobconf, sparkcontext)
    missing  = changemodels.diff(existing, jobconf, sparkcontext)
    created  = changemodels.make(missing, rdd, sparkcontext)
    return changemodels.merge([created, existing], sparkcontext)


def product_dates(rdd, jobconf, sparkcontext):
    return rdd.cartesian(sc.parallelize(jobconf['product_dates'].value))


def seglength(rdd, jobconf, sparkcontext):

    kwargs = {'models': result_to_models(rdd['results']),
              'ord_date': dates.to_ordinal(rdd['product_date']),
              'bot': dates.to_ordinal(dates.startdate(rdd['acquired']))}

    return rdd.map(safely(func=fp.seglength,
                          kwargs=kwargs,
                          chip_x=rdd['chip_x'],
                          chip_y=rdd['chip_y'],
                          x=rdd['x'],
                          y=rdd['y'],
                          alg=algorithm('seglength', fp.version),
                          datestr=rdd['date'],
                          errors=rdd['errors']))\
                          .setName(algorithm('seglength', '20170608'))


def changemag(rdd, jobconf, sparkcontext):

    kwargs = {'models': result_to_models(rdd['results']),
              'ord_date': dates.to_ordinal(rdd['product_date'])}

    return rdd.map(safely(func=fp.changemag,
                          kwargs=kwargs,
                          chip_x=rdd['chip_x'],
                          chip_y=rdd['chip_y'],
                          x=rdd['x'],
                          y=rdd['y'],
                          alg=algorithm('changemag', fp.version),
                          datestr=rdd['date'],
                          errors=rdd['errors']))\
                          .setName(algorithm('changemag', '20170608'))


def changedate(rdd, jobconf, sparkcontext):

    kwargs = {'models': result_to_models(rdd['results']),
              'ord_date': dates.to_ordinal(rdd['product_date'])}

    return rdd.map(safely(func=fp.changedate,
                          kwargs=kwargs,
                          chip_x=rdd['chip_x'],
                          chip_y=rdd['chip_y'],
                          x=rdd['x'],
                          y=rdd['y'],
                          alg=algorithm('changedate', fp.version),
                          datestr=rdd['datestr'],
                          error=rdd['error']))\
                          .setName(algorithm('changedate', fp.version))


def curveqa(rdd, jobconf, sparkcontext):

    kwargs = {'models': result_to_models(rdd['results']),
              'ord_date': dates.to_ordinal(rdd['product_date'])}

    return rdd.map(safely(func=fp.curveqa,
                          kwargs=kwargs,
                          chip_x=rdd['chip_x'],
                          chip_y=rdd['chip_y'],
                          x=rdd['x'],
                          y=rdd['y'],
                          alg=algorithm('curveqa', fp.version),
                          datestr=rdd['date'],
                          errors=rdd['errors']))\
                          .setName(algorithm('curveqa', '20170608'))


def lastchange(change_models, jobconf, sparkcontext):

    kwargs = {'models': result_to_models(rdd['results']),
              'ord_date': dates.to_ordinal(rdd['product_date'])}

    return rdd.map(safely(func=fp.lastchange,
                          kwargs=kwargs,
                          chip_x=rdd['chip_x'],
                          chip_y=rdd['chip_y'],
                          x=rdd['x'],
                          y=rdd['y'],
                          alg=algorithm('lastchange', fp.version),
                          datestr=rdd['date'],
                          errors=rdd['errors']))\
                          .setName(algorithm('lastchange', '20170608'))
