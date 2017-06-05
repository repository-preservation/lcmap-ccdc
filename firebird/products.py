import numpy as np
from datetime import date
from datetime import datetime
from firebird import datastore as ds

version = '0.1'


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
        magnitudes = [models[i]['magnitude'] for i in models if i in ('nir', 'swir1', 'swir2', 'green', 'red')]
        for m in models:
            break_date = date.fromordinal(m['break_day'])
            if (query_date.year == break_date.year) and m['change_probability'] == 1:
                ret = np.linalg.norm(magnitudes)
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


def seglength(models, ord_date, bot):
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


def curveqa(models, ord_date):
    ret = 0
    if ord_date > 0:
        for m in models:
            if m['start_day'] <= ord_date <= m['end_day']:
                ret = m['curve_qa']
                break
    return ret
