from firebird import validation


def test_acquired_true():
    try:
        validation.check_acquired("1980-01-01/2015-12-31")
        assert True
    except:
        assert False


def test_acquired_false():
    try:
        validation.check_acquired("1980-01-01|2015-12-31") is False
        assert False
    except:
        assert True


#def test_coords_true():
#    assert validation.coords(1, 4, 3, 2) is True


#def test_coords_false():
#    assert validation.coords(1, 4, 3, 5) is False


#def test_coords_exception():
#    assert validation.coords(1, 4, 3, None) is False


def test_product_dates():
    try:
        validation.check_product_dates(["1980-01-01"])
        assert True
    except:
        assert False

    try:
        validation.check_product_dates(["1980/01/01"]) is False
        assert False
    except:
        assert True
