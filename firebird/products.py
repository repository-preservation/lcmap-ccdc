import numpy as np
import json
from datetime import date
from .cassandra import execute as cassandra_execute
from .cassandra import RESULT_INPUT
from .cassandra import INSERT_CQL
from datetime import datetime

from firebird import BEGINNING_OF_TIME


def result_to_models(inresult):
    '''
    Function to extract the change_models dictionary from the CCD results
    :param inresult: CCD result object
    :return: dict
    '''
    _rjson = json.loads(inresult['result'])
    return _rjson['change_models']


def lastchange(models, ord_date):
    ret = 0
    if ord_date > 0:
        break_dates = []
        for m in models:
            if m['change_probability'] == 1:
                break_dates.append(m['break_day'])

        diff = [(ord_date - d) for d in break_dates if (ord_date - d) > 0]

        if diff:
           ret = min(diff)

    return ret


def changemag(models, ord_date):
    ret = 0
    if ord_date > 0:
        query_date = date.fromordinal(ord_date)
        for m in models:
            break_date = date.fromordinal(m['break_day'])
            if (query_date.year == break_date.year) and m['change_probability'] == 1:
                ret = np.linalg.norm(m['magnitudes'][1:-1])
                break

    return ret


def changedate(models, ord_date):
    ret = 0
    if ord_date > 0:
        query_date = date.fromordinal(ord_date)
        for m in models:
            break_date = date.fromordinal(m['break_day'])
            if (query_date.year == break_date.year) and m['change_probability'] == 1:
                ret = break_date.timetuple().tm_yday
                break
    return ret


def seglength(models, ord_date, bot=BEGINNING_OF_TIME):
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


def qa(models, ord_date):
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
    _results = list()

    if alg in ('all', 'lastchange'):
        _prods['lastchange'] = lastchange(models, ord_date)
    if alg in ('all', 'changemag'):
        _prods['changemag'] = changemag(models, ord_date)
    if alg in ('all', 'changedate'):
        _prods['changedate'] = changedate(models, ord_date)
    if alg in ('all', 'seglength'):
        _prods['seglength'] = seglength(models, ord_date)
    if alg in ('all', 'qa'):
        _prods['qa'] = qa(models, ord_date)

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
        _results.append(_r)

    # save results
    cassandra_execute(INSERT_CQL, _results)
    return True
