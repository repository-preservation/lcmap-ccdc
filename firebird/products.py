import numpy as np
import json
from datetime import date
from .cassandra import insert_statement
from .cassandra import execute as cassandra_execute
from .cassandra import RESULT_INPUT
from datetime import datetime

beginning_of_time = date(year=1982, month=1, day=1).toordinal()


def result_to_models(inresult):
    _rjson = json.loads(inresult['result'])
    return _rjson['change_models']


def lastchange_val(models, ord_date):
    ret = 0
    if ord_date > 0:
        break_dates = []
        for m in models:
            if m['change_probability'] == 1:
                break_dates.append(m['break_day'])
            else:
                break_dates.append(ord_date)

        diff = [(ord_date - d) for d in break_dates if (ord_date - d) > 0]

        if diff:
           ret = min(diff)

    return ret


def changemag_val(models, ord_date):
    ret = 0
    if ord_date > 0:
        query_date = date.fromordinal(ord_date)
        for m in models:
            #if m['break_day'] <= 0:
            #    continue
            break_date = date.fromordinal(m['break_day'])
            if query_date.year == break_date.year:
                ret = np.linalg.norm(m['magnitudes'][1:-1])
                break

    return ret


def changedate_val(models, ord_date):
    ret = 0
    if ord_date > 0:
        query_date = date.fromordinal(ord_date)
        for m in models:
            #if m['break_day'] <= 0:
            #    continue
            break_date = date.fromordinal(m['break_day'])
            if query_date.year == break_date.year:
                ret = break_date.timetuple().tm_yday
                break
    return ret


def seglength_val(models, ord_date, bot=beginning_of_time):
    ret = 0
    if ord_date > 0:
        all_dates = [bot]
        for m in models:
            all_dates.append(m['start_day'])
            all_dates.append(m['end_day'])

        diff = [(ord_date - d) for d in all_dates if (ord_date - d) > 0]

        if diff:
           ret = min(diff)

    return ret


def qa_val(models, ord_date):
    ret = 0
    if ord_date > 0:
        for m in models:
            if m['start_day'] <= ord_date <= m['end_day']:
                ret = m['qa']
                break
    return ret


def run(alg, ccdres, ord_date):
    '''
    Function for calculating and persisting level2 products from CCD results
    :param alg: Name of algorithm to run
    :param ccdres: CCD result object
    :param ord_date: Date to use for level2 product calculation
    :return: True
    '''
    try:
        models = result_to_models(ccdres)
    except Exception:
        return "json load error for ccdresult: {}".format(ccdres)

    _prods = dict()
    _stmts = list()

    if alg in ('all', 'lastchange'):
        _prods['lastchange'] = lastchange_val(models, ord_date)
    if alg in ('all', 'changemag'):
        _prods['changemag'] = changemag_val(models, ord_date)
    if alg in ('all', 'changedate'):
        _prods['changedate'] = changedate_val(models, ord_date)
    if alg in ('all', 'seglength'):
        _prods['seglength'] = seglength_val(models, ord_date)
    if alg in ('all', 'qa'):
        _prods['qa'] = qa_val(models, ord_date)

    _now = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    for _p in _prods:
        _r = RESULT_INPUT.copy()
        _r['tile_x']          = ccdres['tile_x']
        _r['tile_y']          = ccdres['tile_y']
        _r['x']               = ccdres['x']
        _r['y']               = ccdres['y']
        _r['algorithm']       = _p
        _r['result']          = _prods[_p]
        _r['result_ok']       = True
        _r['result_produced'] = _now
        _stmts.append(insert_statement(_r))

    # save results
    cassandra_execute(_stmts)
    return True
