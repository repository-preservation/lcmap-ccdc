import os
import sys
import json
import logging
import pickle
import datetime as dt
import multiprocessing as mp

from osgeo import gdal
import numpy as np
import xgboost as xgb
from xgboost.core import Booster

log = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(processName)s: %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

cpu = 20
inroot = ''
auxroot = ''
aux_files = {'trends': os.path.join(auxroot, 'trends.tif'),
             'dem': os.path.join(auxroot, 'dem.tif'),
             'slope': os.path.join(auxroot, 'slope.tif'),
             'aspect': os.path.join(auxroot, 'aspect.tif'),
             'posidex': os.path.join(auxroot, 'posidex.tif'),
             'mpw': os.path.join(auxroot, 'mpw.tif'),
             'nlcd2001': os.path.join(auxroot, 'nlcd2001_all_eroded.tif'),
             'nlcd2011': os.path.join(auxroot, 'nlcd2011_all_eroded.tif')}

YEARS = tuple(i for i in range(1984, 2018))
CLASS_DATES = tuple(dt.date(year=i, month=7, day=1).toordinal()
                    for i in YEARS)
grass = 3
forest = 4

##############################
# Helpers
##############################
pheno_index = np.asarray([1, 2, 3, 4, 5, 6,
                          9, 10, 11, 12, 13, 14,
                          17, 18, 19, 20, 21, 22,
                          25, 26, 27, 28, 29, 30,
                          33, 34, 35, 36, 37, 38,
                          41, 42, 43, 44, 45, 46,
                          49, 50, 51, 52, 53, 54])
int_index = np.asarray([7, 15, 23, 31, 39, 47, 55])
slop_index = np.asarray([0, 8, 16, 24, 32, 40, 48])
prob_index = np.asarray([68, 69, 70])
mpw_index = np.asarray([67])
rmse_index = np.asarray([56, 57, 58, 59, 60, 61, 62])
dems_index = np.asarray([63, 64, 65, 66])


def get_affine(filepath):
    ds = gdal.Open(filepath, gdal.GA_ReadOnly)
    return ds.GetGeoTransform()


def geoto_rowcol(x, y, affine):
    'ul_x x_res rot_1 ul_y rot_2 y_res'
    col = (x - affine[0]) / affine[1]
    row = (y - affine[3]) / affine[5]

    return int(row), int(col)


def get_aux(chip_x, chip_y, exclude=None):
    ret = {}
    if not exclude:
        exclude = []

    for aux in aux_files:
        if aux in exclude:
            continue
        else:
            affine = get_affine(aux_files[aux])
            row, col = geoto_rowcol(chip_x, chip_y, affine)
            ds = gdal.Open(aux_files[aux], gdal.GA_ReadOnly)
            arr = ds.GetRasterBand(1).ReadAsArray(col, row, 100, 100)

            if arr is None:
                arr = np.zeros(shape=(100, 100))

        ret[aux] = arr.flatten()

    return ret


def filter_ccd(ccd, begin_ord, end_ord):
    coefs = []
    rmse = []
    starts = []
    ends = []
    idx = []

    results = [filter_result(r, begin_ord, end_ord) if r else None for r in ccd]

    for i, res in enumerate(results):
        if res:
            coefs.append(res['coefs'])
            rmse.append(res['rmses'])
            starts.append(res['start_day'])
            ends.append(res['end_day'])
            idx.append(i)

    return np.array(coefs), np.array(rmse), np.array(idx, dtype=np.int), np.array(starts), np.array(ends)


def filter_result(result, begin_ord, end_ord):
    models = unpack_result(result)

    for model in models:
        if check_coverage(model, begin_ord, end_ord):
            return model


def unpack_result(result):
    models = []

    if result is None:
        return models

    for model in result['change_models']:
        curveinfo = extract_curve(model)

        models.append({'start_day': model['start_day'],
                       'end_day': model['end_day'],
                       'coefs': curveinfo[0],
                       'rmses': curveinfo[1]})

    return models


def extract_curve(model):
    bands = ('blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'thermal')

    coefs = np.zeros(shape=(len(bands), 8))
    rmse = np.zeros(shape=(len(bands),))

    for i, b in enumerate(bands):
        coefs[i] = model[b]['coefficients'] + [model[b]['intercept']]
        rmse[i] = model[b]['rmse']

    return coefs.flatten(), rmse


def check_coverage(model, begin_ord, end_ord):
    return (model['start_day'] <= begin_ord) & (model['end_day'] >= end_ord)


def get_json(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    else:
        return None


def get_chippath(horiz, vert, chip_x, chip_y):
    return os.path.join(inroot,
                        'h{:02d}v{:02d}'.format(horiz, vert),
                        'json',
                        'h{:02d}v{:02d}_{}_{}.json'.format(horiz, vert,
                                                           chip_x, chip_y))


def get_jsonchip(horiz, vert, chip_x, chip_y):
    path = get_chippath(horiz, vert, chip_x, chip_y)

    return load_jsondata(get_json(path))


def load_jsondata(data):
    outdata = np.full(fill_value=None, shape=(100, 100), dtype=object)

    if data is not None:
        for d in data:
            result = d.get('result', 'null')

            # Could leverage geo_utils to do this
            col = int((d['x'] - d['chip_x']) / 30)
            row = int((d['chip_y'] - d['y']) / 30)

            outdata[row][col] = json.loads(result)

    return outdata


def avg_refl(ind, ord_date):
    if len(ind.shape) == 2:
        return ind[:, slop_index] * ord_date + ind[:, int_index]
    else:
        return ind[slop_index] * ord_date + ind[int_index]


def nbrslope(ccdmodel):
    nir_st = ccdmodel['coefs'][slop_index[3]] * ccdmodel['start_day'] + ccdmodel['coefs'][int_index[3]]
    nir_en = ccdmodel['coefs'][slop_index[3]] * ccdmodel['end_day'] + ccdmodel['coefs'][int_index[3]]

    swir_st = ccdmodel['coefs'][slop_index[4]] * ccdmodel['start_day'] + ccdmodel['coefs'][int_index[4]]
    swir_en = ccdmodel['coefs'][slop_index[4]] * ccdmodel['end_day'] + ccdmodel['coefs'][int_index[4]]

    nbr_st = (nir_st - swir_st) / (nir_st + swir_st)
    nbr_en = (nir_en - swir_en) / (nir_en + swir_en)

    return (nbr_en - nbr_st) / (ccdmodel['end_day'] - ccdmodel['start_day'])


def nbrdiff(ccdmodel):
    # Should re-use avg_refl func
    nir_st = ccdmodel['coefs'][slop_index[3]] * ccdmodel['start_day'] + ccdmodel['coefs'][int_index[3]]
    nir_en = ccdmodel['coefs'][slop_index[3]] * ccdmodel['end_day'] + ccdmodel['coefs'][int_index[3]]

    swir_st = ccdmodel['coefs'][slop_index[4]] * ccdmodel['start_day'] + ccdmodel['coefs'][int_index[4]]
    swir_en = ccdmodel['coefs'][slop_index[4]] * ccdmodel['end_day'] + ccdmodel['coefs'][int_index[4]]

    nbr_st = (nir_st - swir_st) / (nir_st + swir_st)
    nbr_en = (nir_en - swir_en) / (nir_en + swir_en)

    return nbr_en - nbr_st


def result_template():
    return {'start_day': 0,
            'end_day': 0,
            'class_vals': [],
            'class_probs': []}


def xg_predict(xgmodel, X):
    # get prediction, this is in 1D array, need reshape to (ndata, nclass)
    # pred_prob = bst.predict(xg_test).reshape(test_Y.shape[0], 6)
    if len(X.shape) < 2:
        X = X.reshape(1, -1)

    data = xgb.DMatrix(X)

    return xgmodel.predict(data)[0]


def hv_affine(h, v):
    xmin = -2565585 + h * 5000 * 30
    ymax = 3314805 - v * 5000 * 30
    return xmin, 30, 0, ymax, 0, -30


def rowcolto_geo(row, col, affine):
    x = affine[0] + col * affine[1]
    y = affine[3] + row * affine[5]

    return x, y


def growthforest():
    ret = np.zeros(shape=(9,), dtype=np.float)
    ret[forest] = 1.11
    ret[grass] = 1.10
    return ret


def growthgrass():
    ret = np.zeros(shape=(9,), dtype=np.float)
    ret[forest] = 1.10
    ret[grass] = 1.11
    return ret


def declineforest():
    ret = np.zeros(shape=(9,), dtype=np.float)
    ret[forest] = 1.21
    ret[grass] = 1.20
    return ret


def declinegrass():
    ret = np.zeros(shape=(9,), dtype=np.float)
    ret[forest] = 1.20
    ret[grass] = 1.21
    return ret


def curr_prev_yr(ord_date):
    d = dt.date.fromordinal(ord_date)
    curr_yr = dt.date(year=d.year, month=1, day=1).toordinal()
    before_yr = dt.date(year=d.year - 1, month=12, day=31).toordinal()

    return curr_yr, before_yr


def classifyccd(ccdmodel, xgmodel, static):
    stack = np.hstack(([ccdmodel['coefs'],
                        ccdmodel['rmses'],
                        static]))

    ret = []
    probs = []
    dates = []
    for d in CLASS_DATES:
        if ccdmodel['start_day'] <= d <= ccdmodel['end_day']:
            # log.debug('Classifying Model')
            temp = np.copy(stack)
            temp[int_index] = avg_refl(stack, d)
            probs.append(xg_predict(xgmodel, temp))
            dates.append(d)

    if len(dates) == 0:
        # log.debug('Unable to classify model ...')
        return ret

    probs = np.array(probs)
    preds = np.argmax(probs, axis=1)
    vals = tuple(range(probs.shape[1]))

    # sl = nbrslope(ccdmodel)
    sl = nbrdiff(ccdmodel)
    if sl > 0.05 and preds[0] == grass and preds[-1] == forest:
        # Grass -> Forest
        # Determine where to split
        spl_idx = np.flatnonzero(preds == forest)[0]
        curr_yr, prev_yr = curr_prev_yr(dates[spl_idx])

        forest_result = result_template()
        forest_result['start_day'] = curr_yr
        forest_result['end_day'] = ccdmodel['end_day']
        forest_result['class_vals'] = vals
        forest_result['class_probs'] = growthforest()
        ret.append(forest_result)

        grass_result = result_template()
        grass_result['start_day'] = ccdmodel['start_day']
        grass_result['end_day'] = prev_yr
        grass_result['class_vals'] = vals
        grass_result['class_probs'] = growthgrass()
        ret.append(grass_result)

    elif sl < -0.05 and preds[-1] == grass and preds[0] == forest:
        # Forest -> Grass
        spl_idx = np.flatnonzero(preds == grass)[0]
        curr_yr, prev_yr = curr_prev_yr(dates[spl_idx])

        forest_result = result_template()
        forest_result['start_day'] = ccdmodel['start_day']
        forest_result['end_day'] = prev_yr
        forest_result['class_vals'] = vals
        forest_result['class_probs'] = declineforest()
        ret.append(forest_result)

        grass_result = result_template()
        grass_result['start_day'] = curr_yr
        grass_result['end_day'] = ccdmodel['end_day']
        grass_result['class_vals'] = vals
        grass_result['class_probs'] = declinegrass()
        ret.append(grass_result)

    else:
        # Normal
        result = result_template()
        result['start_day'] = ccdmodel['start_day']
        result['end_day'] = ccdmodel['end_day']
        result['class_vals'] = vals
        result['class_probs'] = np.mean(probs, axis=0)
        ret.append(result)

    return ret


def worker(model, h, v, output_path, proc_que, out_q):
    params = {'nthread': 1}
    model = Booster(params=params, model_file=model)

    while True:
        chip_x, chip_y = proc_que.get()

        if chip_x == 'kill':
            log.debug('Received kill')
            out_q.put('killed')
            break

        outfile = os.path.join(output_path,
                               'H{:02d}V{:02d}_{}_{}_class.p'.format(h,
                                                                     v,
                                                                     chip_x,
                                                                     chip_y))

        if os.path.exists(outfile):
            log.debug('Output exists, skipping')

        try:
            log.debug('Getting JSON data')
            jn = get_jsonchip(h, v, chip_x, chip_y).flatten()

            log.debug('Getting Aux data')
            aux = get_aux(chip_x, chip_y, exclude=['trends', 'nlcd2001', 'nlcd2011'])

            log.debug('Classifying')
            results = []
            for i in range(jn.shape[0]):
                ccd_models = unpack_result(jn[i])

                dat = np.hstack((aux['dem'][i],
                                 aux['aspect'][i],
                                 aux['slope'][i],
                                 aux['posidex'][i],
                                 aux['mpw'][i],
                                 ))

                res = []
                for ccd_model in ccd_models:
                    res.extend(classifyccd(ccd_model, model, dat))

                results.append(res)
            log.debug('Saving file {}'.format(outfile))
            pickle.dump(results, open(outfile, 'wb'))
        except Exception as e:
            log.debug('EXCEPTION with {} {}'.format(chip_x, chip_y))
            log.exception(e)


def master_proc(h, v, outpath, wfunc, model, num_workers):
    tile_affine = hv_affine(h, v)
    queue = mp.Queue()
    out_q = mp.Queue()
    log.debug('Creating input queue')
    for row in range(0, 5000, 100):
        for col in range(0, 5000, 100):
            chip_x, chip_y = rowcolto_geo(row, col, tile_affine)

            if os.path.exists(os.path.join(outpath,
                                           'H{:02d}V{:02d}_{}_{}_class.p'.format(
                                                   h, v, chip_x,
                                                   chip_y))):
                continue

            queue.put((chip_x, chip_y))

    for _ in range(num_workers):
        queue.put(('kill', 'kill'))

    log.debug('Starting workers')
    for _ in range(num_workers):
        mp.Process(target=wfunc,
                   args=(model, h, v, outpath, queue, out_q),
                   name='Process-{}'.format(_)).start()

    listener(out_q, num_workers)


def listener(q, num_workers):
    count = 0
    while True:
        log.debug('Kill count: {}'.format(count))

        if count >= num_workers:
            break

        msg = q.get()

        if msg == 'killed':
            count += 1


def classify(modelpath, outpath, h, v):
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    master_proc(h, v, outpath, worker, modelpath, cpu)


if __name__ == '__main__':
    classify(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
