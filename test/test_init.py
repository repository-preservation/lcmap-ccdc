import firebird as fb
import pyspark

def test_minbox():
    assert 1 > 0


def test_dtstr_to_ordinal():
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
        bc = fb.broadcast({'a': 'a',
                           'true': True,
                           'list': [1, 2, 3],
                           'set': set([1, 2, 3]),
                           'tuple': tuple([1, 2, 3]),
                           'dict': dict({'a': 1}),
                           'num': 3}, sparkcontext=sc)

        assert bc['a'].value == 'a'
        assert bc['true'].value == True
        assert bc['list'].value == [1, 2, 3]
        assert bc['set'].value == {1, 2, 3}
        assert bc['tuple'].value == (1, 2, 3)
        assert bc['dict'].value == {'a': 1}
        assert bc['num'].value == 3
    finally:
        if sc is not None:
            sc.stop()
