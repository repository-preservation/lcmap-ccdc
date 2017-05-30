from firebird import validation


def test_acquired_true():
    assert validation.acquired("1980-01-01/2015-12-31") is True


def test_acquired_false():
    assert validation.acquired("1980-01-01|2015-12-31") is False


def test_coords_true():
    assert validation.coords(1, 4, 3, 2) is True


def test_coords_false():
    assert validation.coords(1, 4, 3, 5) is False


def test_coords_exception():
    assert validation.coords(1, 4, 3, None) is False


def test_prod_true():
    assert validation.prod("1980-01-01") is True


def test_prod_false():
    assert validation.prod("1980/01/01") is False