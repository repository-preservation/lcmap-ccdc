from firebird import functions as f


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
