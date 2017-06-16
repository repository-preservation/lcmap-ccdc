"""functions.py is a module of generalized, reusable functions"""

from functools import singledispatch
import functools
import hashlib
import itertools
import json
import numpy as np


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
    >>> inputs = [1, (2, 3), 4]
    >>> extract(inputs, [0])
    >>> 1
    >>> extract(inputs, [1])
    >>> (2, 3)
    >>> extract(inputs, [1, 0])
    >>> 2
    >>> extract(inputs, [1, 1])
    >>> 3
    ...
    """
    t = tuple(elements)
    if len(t) == 0 or not getattr(sequence, '__iter__', False):
        return sequence
    else:
        seq = sequence[t[0]]
        rest = t[1:len(t)]
        return extract(seq, rest)


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


def broadcast(context, sparkcontext):
    """Sets read-only values on the cluster to make them available to cluster
    operations.
    :param context: dict of key: values to broadcast to the cluster
    :param sparkcontext: An active spark context for the spark cluster
    :return: dict of cluster references for each key: value pair
    """
    return {k: sparkcontext.broadcast(value=v) for k,v in context.items()}


@singledispatch
def serialize(arg):
    """Converts datastructure to json, computes digest
    :param dictionary: A python dict
    :return: Tuple of digest, json
    """
    s = json.dumps(arg, sort_keys=True)
    return digest(s), s


@serialize.register(set)
def _(arg):
    """Converts set to list then serializes the resulting value
    """
    return serialize(list(arg))


def deserialize(string):
    """Reconstitues datastructure from a string.
    :param string: A serialized data structure
    :return: Data structure represented by :param string:
    """
    return json.loads(string)
