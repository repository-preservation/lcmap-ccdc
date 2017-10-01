""" WIP """

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
    return '{}_{}'.format(name, version)


def result(chip_x, chip_y, x, y, alg, datestr):
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

    if 'error' in result:
        msg = 'upstream->{}:{}'
        return merge(result, {'error': msg.format(alg, get('error', result))})
    else:
        return False


def tryexcept(func, kwargs, chip_x, chip_y, x, y, alg, datestr):

    try:
        return success(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                       datestr=datestr, data=func(**kwargs))
    except Exception as errs:
        return error(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                     datestr=datestr, data=errs)


def safely(func, kwargs, chip_x, chip_y, x, y, alg, datestr, errors):

    return (haserrors(chip_x=chip_x, chip_y=chip_y, x=x, y=y, alg=alg,
                      datestr=datestr, errors=errors) or
            tryexcept(func=func, kwargs=kwargs, chip_x=chip_x, chip_y=chip_y,
                      x=x, y=y, alg=alg, datestr=datestr))


def simplify_detect_results(results):

    def simplify(result):
        return {k: f.simplify_objects(v) for k, v in result.items()}

    return simplify(results) if type(results) is dict else dict()


def result_to_models(result):
    return simplify_detect_results(result).get('change_models')


def pyccd(rdd):

    kwargs = merge(get(rdd, 'result', {}), {'params': ccd_params()})

    return safely(func=ccd.detect,
                  kwargs=kwargs,
                  chip_x=rdd['chip_x'],
                  chip_y=rdd['chip_y'],
                  x=rdd['x'],
                  y=rdd['y'],
                  alg=ccd.algorithm,
                  datestr=rdd['acquired'],
                  errors=rdd['errors'])


def lastchange(rdd):

    chip_x = rdd[0][0][0]
    chip_y = rdd[0][0][1]
    x = rdd[0][0][2]
    y = rdd[0][0][3]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data),
              'ord_date': dates.to_ordinal(date)}

    return safely(func=fp.lastchange, kwargs=kwargs, chip_x=chip_x,
                  chip_y=chip_y, x=x, y=y,
                  alg=algorithm('lastchange', fp.version), datestr=date,
                  errors=errs)


def changemag(rdd):

    chip_x = rdd[0][0][0]
    chip_y = rdd[0][0][1]
    x = rdd[0][0][2]
    y = rdd[0][0][3]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data),
              'ord_date': dates.to_ordinal(date)}

    return safely(func=fp.changemag, kwargs=kwargs, x=x, y=y, chip_x=chip_x,
                  chip_y=chip_y, alg=algorithm('changemag', fp.version),
                  datestr=date, errors=errs)


def changedate(rdd):

    return safely(func=fp.changedate,
                  kwargs={'models': result_to_models(rdd['data']),
                          'ord_date': dates.to_ordinal(rdd['datestr'])},
                  x=rdd['x'],
                  y=rdd['y'],
                  chip_x=rdd['chip_x'],
                  chip_y=rdd['chip_y'],
                  alg=algorithm('changedate', fp.version),
                  datestr=rdd['datestr'],
                  error=rdd['error'])


def seglength(rdd):

    chip_x = rdd[0][0][0]
    chip_y = rdd[0][0][1]
    x = rdd[0][0][2]
    y = rdd[0][0][3]
    acquired = rdd[0][0][5]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data),
              'ord_date': dates.to_ordinal(date),
              'bot': dates.to_ordinal(dates.startdate(acquired))}

    return safely(func=fp.seglength, kwargs=kwargs, x=x, y=y, chip_x=chip_x,
                  chip_y=chip_y, alg=algorithm('seglength', fp.version),
                  datestr=date, errors=errs)


def curveqa(rdd):

    chip_x = rdd[0][0][0]
    chip_y = rdd[0][0][1]
    x = rdd[0][0][2]
    y = rdd[0][0][3]
    data = rdd[0][1]
    errs = rdd[0][2]
    date = rdd[1]
    kwargs = {'models': result_to_models(data),
              'ord_date': dates.to_ordinal(date)}

    return safely(func=fp.curveqa, kwargs=kwargs, x=x, y=y,  chip_x=chip_x,
                  chip_y=chip_y, alg=algorithm('curveqa', fp.version),
                  datestr=date, errors=errs)


def fits_in_box(value, bbox):

    def fits(point, bbox):
        _, _, x, y = point
        return (float(x) >= float(bbox['ulx']) and
                float(x) <= float(bbox['lrx']) and
                float(y) >= float(bbox['lry']) and
                float(y) <= float(bbox['uly']))

    return bbox is None or fits(value[0], bbox)


def labels(inputs=None, ccd=None, lastchange=None, changemag=None,
           changedate=None, seglength=None, curveqa=None):

    return {'inputs': inputs, 'ccd': ccd, 'lastchange': lastchange,
            'changemag': changemag, 'changedate': changedate,
            'seglength': seglength, 'curveqa': curveqa}


def products(jobconf, sparkcontext):

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

    _ccd = _in.map(pyccd).setName(ccd.algorithm).persist()

    # cartesian will create an rdd that looks like:
    # ((((chip_x, chip_y), x, y, alg, product_date_str), data), product_date)
    _ccd_dates = _ccd.cartesian(sc.parallelize(jobconf['product_dates'].value))

    _lc = _ccd_dates.map(lastchange).setName(algorithm('lastchange',
                                                       '20170608'))
    _cm = _ccd_dates.map(changemag).setName(algorithm('changemag',
                                                      '20170608'))
    _cd = _ccd_dates.map(changedate).setName(algorithm('changedate',
                                                       '20170608'))
    _sl = _ccd_dates.map(seglength).setName(algorithm('seglength',
                                                      '20170608'))
    _qa = _ccd_dates.map(curveqa).setName(algorithm('curveqa',
                                                    '20170608'))

    return labels(inputs=_in, ccd=_ccd, lastchange=_lc, changemag=_cm,
                  changedate=_cd, seglength=_sl, curveqa=_qa)


def chipids(jobconf, sparkcontext):
    return sc.parallelize(
               jobconf['chip_ids'].value,
               jobconf['initial_partitions'].value).setName('chipids')


def timeseries(chip_ids, jobconf, sparkcontext):
    return inputs.get(chip_ids, jobconf, sparkcontext)


def changemodels(time_series, jobconf, sparkcontext):
    existing = changemodels.get(jobconf, sparkcontext)
    missing = changemodels.diff(existing, jobconf, sparkcontext)
    new = changemodels.make(missing, timeseries, sparkcontext)
    return changemodels.merge([new, existing], sparkcontext)


def seglength(change_models, jobconf, sparkcontext):
    pass


def changemag(change_models, jobconf, sparkcontext):
    pass


def changedate(change_models, jobconf, sparkcontext):
    pass


def curveqa(change_models, jobconf, sparkcontext):
    pass


def lastchange(change_models, jobconf, sparkcontext):
    pass
