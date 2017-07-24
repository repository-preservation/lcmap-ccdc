"""functions.py is a module of generalized, reusable functions"""

from collections import OrderedDict
from functools import reduce
from functools import singledispatch
import functools
import hashlib
import itertools
import json
import numpy as np
import re


def first(sequence):
    """
    Returns the first element of the sequence
    :param sequence: A Python sequence
    :return: The first element of the sequence
    """
    return sequence[0]


def rest(sequence):
    """
    Returns all but the first element of the sequence
    :param sequence: A Python sequence
    :return: All elements of the sequence except the first
    """
    return tuple(sequence[1:len(sequence)])


def compose(*functions):
    def compose2(f, g):
        return lambda x: f(g(x))
    return functools.reduce(compose2, functions, lambda x: x)


def extract(sequence, elements):
    """Given a sequence (possibly with nested sequences), extract
    the element identifed by the elements sequence.
    :param sequence: A sequence of elements which may be other sequences
    :param elements: Sequence of nested element indicies (in :param sequence:)
                     to extract
    :return: The target element
    Example:
    >>> inputs = [1, (2, 3, (4, 5)), 6]
    >>> extract(inputs, [0])
    >>> 1
    >>> extract(inputs, [1])
    >>> (2, 3, (4, 5))
    >>> extract(inputs, [1, 0])
    >>> 2
    >>> extract(inputs, [1, 1])
    >>> 3
    >>> extract(inputs, [1, 2])
    >>> (4, 5)
    >>> extract(inputs, [1, 2, 0])
    >>> 4
    ...
    """
    e = tuple(elements)
    if len(e) == 0 or not getattr(sequence, '__iter__', False):
        return sequence
    else:
        seq = sequence[first(e)]
        return extract(seq, rest(e))


def flatten(iterable):
    """Reduce dimensionality of iterable containing iterables
    :param iterable: A multi-dimensional iterable
    :returns: A one dimensional iterable
    """
    return itertools.chain.from_iterable(iterable)


def merge(dicts):
    """Combines a sequence of dicts into a single dict.
    :params dicts: A sequence of dicts
    :returns: A single dict
    """
    return {k: v for d in dicts for k, v in d.items()}


def intersection(items):
    """Returns the intersecting set contained in items
    :param items: Two dimensional sequence of items
    :returns: Intersecting set of items
    :example:
    >>> items = [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    >>> intersection(items)
    {3}
    """
    return set.intersection(*(map(lambda x: set(x), items)))


@functools.lru_cache(maxsize=128, typed=True)
def minbox(points):
    """Returns the minimal bounding box necessary to contain points
    :param points: A tuple of (x,y) points: ((0,0), (40, 55), (66, 22))
    :return: dict with ulx, uly, lrx, lry
    """
    x, y = [point[0] for point in points], [point[1] for point in points]
    return {'ulx': min(x), 'lrx': max(x), 'lry': min(y), 'uly': max(y)}


def sha256(string):
    """Computes and returns a sha256 digest of the supplied string
    :param string: string to digest
    :return: digest value
    """
    return hashlib.sha256(string.encode('UTF-8')).hexdigest()


def md5(string):
    """Computes and returns an md5 digest of the supplied string
    :param string: string to digest
    :return: digest value
    """
    return hashlib.md5(string.encode('UTF-8')).hexdigest()


def simplify_objects(obj):
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.int64):
        return int(obj)
    elif isinstance(obj, tuple) and ('_asdict' in dir(obj)):
        # looks like a namedtuple
        _out = {}
        objdict = obj._asdict()
        for key in objdict.keys():
            _out[key] = simplify_objects(objdict[key])
        return _out
    elif isinstance(obj, (list, np.ndarray, tuple)):
        return [simplify_objects(i) for i in obj]
    else:
        # should be a serializable type
        return obj


def sort(iterable, key=None):
    """Sorts an iterable"""
    return sorted(iterable, key=key, reverse=False)


def rsort(iterable, key=None):
    """Reverse sorts an iterable"""
    return sorted(iterable, key=key, reverse=True)


def false(v):
    """Returns true is v is False, 0 or 'False' (case insensitive)
    :param v: A value
    :return: Boolean
    """
    return v is not None and (v == 0 or str(v).strip().lower() == 'false')


def true(v):
    """Returns true is v is True, 1 or 'True' (case insensitive)
    :param v: A value
    :return: Boolean
    """
    return v is not None and (v == 1 or str(v).strip().lower() == 'true')


@singledispatch
def serialize(arg):
    """Converts datastructure to json, computes digest
    :param dictionary: A python dict
    :return: Tuple of digest, json
    """
    s = json.dumps(arg, sort_keys=True, separators=(',', ':'),
                   ensure_ascii=True)
    return md5(s), s


@serialize.register(set)
def _(arg):
    """Converts set to list then serializes the resulting value
    """
    return serialize(sorted(list(arg)))


def deserialize(string):
    """Reconstitues datastructure from a string.
    :param string: A serialized data structure
    :return: Data structure represented by :param string:
    """
    return json.loads(string)


def flip_keys(dods):
    """Accepts a dictionary of dictionaries and flips the outer and inner keys.
    All inner dictionaries must have a consistent set of keys or key Exception
    is raised.
    :param dods: dict of dicts
    :returns: dict of dicts with inner and outer keys flipped
    :example:

    input:

    {'red':   {(0, 0): [110, 110, 234, 664], (0, 1): [23, 887, 110, 111]},
     'green': {(0, 0): [120, 112, 224, 624], (0, 1): [33, 387, 310, 511]},
     'blue':  {(0, 0): [128, 412, 244, 654], (0, 1): [73, 987, 119, 191]},
    ...

    output:
    {(0, 0): {'red':   [110, 110, 234, 664],
              'green': [120, 112, 224, 624],
              'blue':  [128, 412, 244, 654], ... },
     (0, 1), {'red':   [23, 887, 110, 111],
              'green': [33, 387, 310, 511],
              'blue':  [73, 987, 119, 191], ... }}
    ...
    """

    def flip(innerkeys, outerkeys, inputs):
        for ik in innerkeys:
            yield({ik: {ok: inputs[ok][ik] for ok in outerkeys}})

    outerkeys = set(dods.keys())
    innerkeys = set(reduce(lambda accum, v: accum + v,
                           [list(dods[ok].keys()) for ok in outerkeys]))
    return merge(flip(innerkeys, outerkeys, dods))


def cqlstr(string):
    """Makes a string safe to use in Cassandra CQL commands
    :param string: The string to use in CQL
    :return: A safe string replacement
    """
    return re.sub('[-:.]', '_', string)


def represent(item):
    """Represents callables and values consistently
    :param item: The item to represent
    :return: Item representation
    """
    return repr(item.__name__) if callable(item) else repr(item)


def isnumeric(value):
    """Does a string value represent a number (positive or negative?)
    :param value: A string
    :return: True or False
    """
    try:
        float(value)
        return True
    except:
        return False
