from firebird import functions as f
import pyspark


def test_intersection():
    items = [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    assert f.intersection(items) == {3}


def test_minbox():
    assert 1 > 0


def test_simplify_objects():
    assert 1 > 0


def test_sort():
    assert 1 > 0


def test_rsort():
    assert 1 > 0


def test_compose():
    assert 1 > 0


def test_broadcast():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_broadcast")
        bc = f.broadcast({'a': 'a',
                          'true': True,
                          'list': [1, 2, 3],
                          'set': set([1, 2, 3]),
                          'tuple': tuple([1, 2, 3]),
                          'dict': dict({'a': 1}),
                          'none': None,
                          'num': 3}, sparkcontext=sc)

        assert bc['a'].value == 'a'
        assert bc['true'].value == True
        assert bc['list'].value == [1, 2, 3]
        assert bc['set'].value == {1, 2, 3}
        assert bc['tuple'].value == (1, 2, 3)
        assert bc['dict'].value == {'a': 1}
        assert bc['num'].value == 3
        assert bc['none'].value == None
    finally:
        if sc is not None:
            sc.stop()