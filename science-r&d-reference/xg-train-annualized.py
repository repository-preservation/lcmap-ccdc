import os
import sys
import logging
from itertools import starmap, product
import datetime as dt

import numpy as np
from sklearn.model_selection import train_test_split
import xgboost as xgb


log = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(processName)s: %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

npyroot = ''
outroot = ''
cpu = 20

indset = 'ind2001'

xg_params = {'objective': 'multi:softprob',
             'num_class': 9,
             'max_depth': 8,
             'tree_method': 'hist',
             'eval_metric': 'mlogloss',
             'silent': 1,
             'nthread': cpu}
xg_numrnds = 500

sampling = {'class_min': 600000,
            'class_max': 8000000,
            'target_samples': 20000000}


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


def sample(dependent, params=sampling, random_state=np.random.RandomState()):

    class_values, percent = class_stats(dependent)

    # Adjust the target counts that we are wanting based on the percentage
    # that each one represents in the base data set.
    adj_counts = np.ceil(params['target_samples'] * percent)
    adj_counts[adj_counts > params['class_max']] = params['class_max']
    adj_counts[adj_counts < params['class_min']] = params['class_min']

    selected_indices = []
    for cls, count in zip(class_values, adj_counts):
        # Index locations of values
        indices = np.where(dependent == cls)[0]

        # Randomize the order of the index locations
        indices = random_state.permutation(indices)

        # Add the index locations up to the count
        selected_indices.extend(indices[:int(count)])

    return np.array(selected_indices)


def class_stats(dependent):
    vals, cnts = np.unique(dependent, return_counts=True)

    # Find the percentage that each class makes up in the data set
    prct = cnts / np.sum(cnts)

    return vals, prct


def id_gridlocs(horiz, vert):
    return starmap(lambda a, b: (horiz + a, vert + b),
                   product((0, -1, 1), (0, -1, 1)))


def load_dep(h, v):
    path = os.path.join(npyroot, 'h{:02d}v{:02d}_dep2001.npy'.format(h, v))

    if not os.path.exists(path):
        return None, None

    data = np.load(path)
    mask = data != 0

    return data[mask], mask


def load_ind(h, v, mask):
    path = os.path.join(npyroot, 'h{:02d}v{:02d}_{}.npy'.format(h, v, indset))

    data = np.load(path)
    return data[mask]


def avg_refl(ind, ord_date):
    # could leverage reshape, but this is a rush job
    if len(ind.shape) == 2:
        return ind[:, slop_index] * ord_date + ind[:, int_index]
    else:
        return ind[slop_index] * ord_date + ind[int_index]


def checkxginput(train_x, train_y):
    classes = np.unique(train_y)

    if 7 not in classes:
        train_x = np.vstack((train_x, np.zeros(shape=(1, train_x.shape[1]))))
        train_y = np.concatenate((train_y, [7]))
    if 0 not in classes:
        train_x = np.vstack((train_x, np.zeros(shape=(1, train_x.shape[1]))))
        train_y = np.concatenate((train_y, [0]))

    return train_x, train_y


def train(horiz, vert):
    log.debug(f'Begin data gathering for H{horiz} V{vert}')
    hvs = id_gridlocs(horiz, vert)

    dep = []
    ind = []
    for h, v in hvs:
        log.debug(f'Loading data for H{h} V{v}')

        d, mask = load_dep(h, v)
        if d is None:
            log.debug(f'No data for H{h} V{v}')
            continue

        dep.append(d)
        ind.append(load_ind(h, v, mask))

    log.debug('Merging dependent arrays together')
    dep = np.concatenate(dep).astype(np.int8)
    log.debug('Merging independent arrays together')
    ind = np.vstack(ind).astype(np.float32)

    log.debug('Total size of Dependent array: {}'.format(dep.shape))
    log.debug('Total size of Independent array: {}'.format(ind.shape))

    idxs = sample(dep)

    dep = dep[idxs]
    ind = ind[idxs]

    preddate = dt.date(year=2001, month=7, day=1).toordinal()

    ind[:, int_index] = avg_refl(ind, preddate)

    X_train, X_test, y_train, y_test = train_test_split(ind, dep, test_size=0.2)

    log.debug('X_train, y_train shape: {0}, {1}'.format(X_train.shape, y_train.shape))
    log.debug('X_test, y_test shape: {0}, {1}'.format(X_test.shape, y_test.shape))
    log.debug('train_y population: ' + np.array2string(np.unique(y_train, return_counts=True)[0]) + np.array2string(
        np.unique(y_train, return_counts=True)[1]))

    del ind
    del dep

    log.debug('Checking arrays...')
    X_train, y_train = checkxginput(X_train, y_train)
    xgbmat = xgb.DMatrix(X_train, label=y_train)
    xgbval = xgb.DMatrix(X_test, label=y_test)

    log.debug('Training Model')
    watchlist = [(xgbmat, 'train'), (xgbval, 'eval')]
    model = xgb.train(xg_params, xgbmat, xg_numrnds, watchlist, early_stopping_rounds=10, verbose_eval=False)
    log.debug('Finished Training Model')
    log.debug('best ite: {0}'.format(model.best_iteration))
    log.debug('best score (Multiclass classification error rate): {0}'.format(model.best_score))

    log.debug('Saving model ...')
    outfile = os.path.join(outroot, 'h{:02d}v{:02d}-annualized.xgmodel'.format(horiz, vert, indset))
    model.save_model(outfile)

    return None


if __name__ == '__main__':
    train(int(sys.argv[1]), int(sys.argv[2]))
