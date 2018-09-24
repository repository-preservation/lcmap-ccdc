import os
import sys
import re
import json
import logging
from datetime import datetime
from itertools import product, starmap
import multiprocessing as mp

import numpy as np
from osgeo import gdal


log = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(processName)s: %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)


inroot = ''
outroot = ''
cpu = 20
auxroot = '/lcmap_data/bulk/ancillary/training'
aux_files = {'trends': os.path.join(auxroot, 'trends.tif'),
             'dem': os.path.join(auxroot, 'dem.tif'),
             'slope': os.path.join(auxroot, 'slope.tif'),
             'aspect': os.path.join(auxroot, 'aspect.tif'),
             'posidex': os.path.join(auxroot, 'posidex.tif'),
             'mpw': os.path.join(auxroot, 'mpw.tif'),
             'nlcd2001': os.path.join(auxroot, 'nlcd2001_all_eroded.tif'),
             'nlcd2011': os.path.join(auxroot, 'nlcd2011_all_eroded.tif')}


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


def main(horiz, vert):
    locs = list(id_gridlocs(horiz, vert))
    log.debug('Tiles to build: %s', locs)

    tile_ls = [os.path.join(inroot, d) for d in os.listdir(inroot)
               if (checkpath(os.path.join(inroot, d)) and (hvdir(d) in locs))]
    pool = mp.Pool(cpu)

    log.debug('Paths identified: %s', tile_ls)

    for tile in tile_ls:
        gather(tile, outroot, pool)


def id_gridlocs(horiz, vert):
    return starmap(lambda a, b: (horiz + a, vert + b),
                   product((0, -1, 1), (0, -1, 1)))


def checkpath(path):
    return checkname(os.path.split(path)[-1]) and checkjcount(path)


def checkname(dirname):
    return re.fullmatch('h[0-9][0-9]v[0-9][0-9]', dirname)


def checkjcount(path):
    jpath = os.path.join(path, 'json')

    return os.path.exists(jpath) and len(os.listdir(jpath)) == 2500


def hvdir(dirname):
    h, v = dirname.split('v')
    return int(h[1:]), int(v)


def hv_affine(h, v):
    xmin = -2565585 + h * 5000 * 30
    ymax = 3314805 - v * 5000 * 30
    return xmin, 30, 0, ymax, 0, -30


def tilechiplist(ul):
    """build a list of chips for a tile based on the given UL"""
    return product(range(ul[0], ul[0] + 150000, 3000), range(ul[1], ul[1] - 150000, -3000))


def chippixellist(ul):
    return list(product(range(ul[1], ul[1] - 3000, -30), range(ul[0], ul[0] + 3000, 30)))


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


def intercept2med(intercept, slope, dates):
    return slope * dates.reshape(-1, 1) + intercept


def freq2TS_mk2(freq, slopes, intercepts, date, coefM):
    # print('seasonal matrix: ', freq.shape)
    # print('coefM: ', coefM.shape)
    yearL = np.asarray([datetime(datetime.fromordinal(d).year,1,1,0,0).toordinal() for d in date])
    dayM = np.zeros((len(date), 8))
    for iday in range(8):
        dayM[:, iday] = yearL + iday * int(366/7)

    if freq.shape[1] % coefM.shape[0] != 0:
        print('dimension is wrong:', freq.shape, coefM.shape)
        return
    bands = int(freq.shape[1]/coefM.shape[0])
    n_freq = coefM.shape[0]
    n_sample = dayM.shape[1]
    modelTS = np.zeros((freq.shape[0], bands * n_sample))
    # print('modelTS: ', modelTS.shape)
    for iband in range(bands):
        modelTS[:,iband*n_sample:(iband + 1)*n_sample] = np.dot(freq[:,iband*n_freq:(iband + 1)*n_freq], coefM) + np.multiply(slopes[:, iband][:, np.newaxis], dayM) + intercepts[:, iband][:, np.newaxis]

    return modelTS


def coefficient_matrix_8():
    avg_days_yr = 365.25
    w = 2 * np.pi / avg_days_yr
    dates = np.array(range(1,366,int(366/7))) # so far total 8 samples per year

    matrix = np.zeros(shape=(len(dates), 6), order='F')

    matrix[:, 0] = np.cos(w * dates)
    matrix[:, 1] = np.sin(w * dates)
    matrix[:, 2] = np.cos(2 * w * dates)
    matrix[:, 3] = np.sin(2 * w * dates)
    matrix[:, 4] = np.cos(3 * w * dates)
    matrix[:, 5] = np.sin(3 * w * dates)

    return matrix.T


def get_chipdata(args):
    log.debug(f'Received {args}')
    h, v, chip_x, chip_y = args

    xy = np.fliplr(chippixellist((chip_x, chip_y)))

    ret = {}
    base = 'h{:02d}v{:02d}'.format(h, v)

    try:
        log.debug('Getting Aux data')
        aux = get_aux(chip_x, chip_y, exclude=['trends'])

        log.debug('Getting JSON data')
        jn = get_jsonchip(h, v, chip_x, chip_y).flatten()
        # NLCD 2001
        coefs, rmse, idxs, starts, ends = filter_ccd(jn, 730120, 731216)
        mask2001 = np.zeros(shape=(10000,), dtype=np.bool)
        count = len(idxs)
        if count:
            mask2001[idxs] = 1

            ret['_'.join([base, 'ind2001'])] = np.hstack((coefs,
                                                          rmse,
                                                          aux['dem'][mask2001][:, np.newaxis],
                                                          aux['aspect'][mask2001][:, np.newaxis],
                                                          aux['slope'][mask2001][:, np.newaxis],
                                                          aux['posidex'][mask2001][:, np.newaxis],
                                                          aux['mpw'][mask2001][:, np.newaxis]))

            ret['_'.join([base, 'dep2001'])] = aux['nlcd2001'][mask2001]
            ret['_'.join([base, 'coords2001'])] = xy[mask2001]

            # # Recenter intercept
            # ret['_'.join([base, 'interc2001'])] = np.copy(ret['_'.join([base, 'ind2001'])])
            # ret['_'.join([base, 'interc2001'])][:, int_index] = intercept2med(ret['_'.join([base, 'interc2001'])][:, int_index],
            #                                                                   ret['_'.join([base, 'interc2001'])][:, slop_index],
            #                                                                   (starts + ends) / 2)

            # # 8 sampling
            # ret['_'.join([base, '8sample2001'])] = np.copy(ret['_'.join([base, 'ind2001'])])
            # ret['_'.join([base, '8sample2001'])][:, :56] = freq2TS_mk2(ret['_'.join([base, '8sample2001'])][:, pheno_index],
            #                                                            ret['_'.join([base, '8sample2001'])][:, slop_index],
            #                                                            ret['_'.join([base, '8sample2001'])][:, int_index],
            #                                                            ((starts + ends) / 2).astype(int),
            #                                                            coefficient_matrix_8())
        #
        # # NLCD 2011
        # coefs, rmse, idxs, starts, ends = filter_ccd(jn, 733773, 734503)
        # mask2011 = np.zeros(shape=(10000,), dtype=np.bool)
        # mask2011[idxs] = 1
        # count = len(idxs)
        # if count:
        #     ret['_'.join([base, 'ind2011'])] = np.hstack((coefs,
        #                                                   rmse,
        #                                                   aux['dem'][mask2011][:, np.newaxis],
        #                                                   aux['aspect'][mask2011][:, np.newaxis],
        #                                                   aux['slope'][mask2011][:, np.newaxis],
        #                                                   aux['posidex'][mask2011][:, np.newaxis],
        #                                                   aux['mpw'][mask2011][:, np.newaxis]))
        #
        #     ret['_'.join([base, 'dep2011'])] = aux['nlcd2011'][mask2011]
        #     ret['_'.join([base, 'coords2011'])] = xy[mask2011]
        #
        #     # Recenter intercept
        #     ret['_'.join([base, 'interc2011'])] = np.copy(ret['_'.join([base, 'ind2011'])])
        #     ret['_'.join([base, 'interc2011'])][:, int_index] = intercept2med(ret['_'.join([base, 'interc2011'])][:, int_index],
        #                                                                       ret['_'.join([base, 'interc2011'])][:, slop_index],
        #                                                                       (starts + ends) / 2)
        #
        #     # 8 sampling
        #     ret['_'.join([base, '8sample2011'])] = np.copy(ret['_'.join([base, 'ind2011'])])
        #     ret['_'.join([base, '8sample2011'])][:, :56] = freq2TS_mk2(ret['_'.join([base, '8sample2011'])][:, pheno_index],
        #                                                                ret['_'.join([base, '8sample2011'])][:, slop_index],
        #                                                                ret['_'.join([base, '8sample2011'])][:, int_index],
        #                                                                ((starts + ends) / 2).astype(int),
        #                                                                coefficient_matrix_8())

    except:
        log.debug(f'Problem with chip x: {chip_x} chip y: {chip_y}')
        log.debug(f'Tile H{h} V{v}')
        raise

    return ret


def checkexist(h, v, outdir):
    path = os.path.join(outdir,
                        '_'.join(['h{:02d}v{:02d}'.format(h, v),
                                  'dep2001.npy']))
    return os.path.exists(path)


def gather(indir, outdir, pool):
    h, v = hvdir(os.path.split(indir)[-1])

    aff = hv_affine(h, v)
    arg_ls = [(h, v, chip[0], chip[1])
              for chip in tilechiplist((aff[0], aff[3]))
              if not checkexist(h, v, outdir)]

    datasets = {}
    log.debug('Gathering Data')
    for chip_result in pool.imap_unordered(get_chipdata, arg_ls):
        if not chip_result:
            continue

        for data in chip_result:
            if data not in datasets:
                datasets[data] = []

            datasets[data].append(chip_result[data])

    log.debug('Done gathering data')
    keys = list(datasets.keys())
    for key in keys:
        log.debug(f'Outputting {key}')
        outfile = os.path.join(outdir, key)

        data = datasets.pop(key)
        if len(data[0].shape) > 1:
            d = np.vstack(data)
        else:
            d = np.concatenate(data)

        np.save(outfile, d)

    log.debug(f'Finished with tile {h} {v}')

    return


if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]))
