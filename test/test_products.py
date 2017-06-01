from firebird import driver
from firebird import products
from firebird import dtstr_to_ordinal as dto
from mock import patch
from test.mocks import aardvark as ma
from test.mocks import chip as mc
from test.mocks import driver as md
from test.shared import simplified_detect_results

@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_ccd():
    inputs = driver.pyccd_inputs((-100200, 300400),
                                 'http://localhost', 'http://localhost',
                                 '1980-01-01/2015-12-31')
    results = products.ccd((inputs[0][0], inputs[0][1]))
    assert type(results[0]) == tuple
    assert type(results[1]) == dict
    assert len(results[0]) == 3


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_ccd_exception():
    _rdd = driver.pyccd_inputs((-100200, 300400),
                               'http://localhost',
                               'http://localhost',
                               '1980-01-01/2015-12-31')
    band_dict = _rdd[0][1]
    band_dict.pop('reds')
    try:
        results = products.ccd(((111111, 222222), band_dict))
    except Exception as e:
        assert e is not None

def test_result_to_models():
    assert products.result_to_models({'result': '{"change_models": "foo bar"}'}) == "foo bar"


def test_lastchange():
    assert products.lastchange([{'change_probability': 1, 'break_day': 999}], 1000) == 1


def test_changemag():
    ord_date = dto("1984-04-01")
    assert products.changemag([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 0.0


def test_changedate():
    ord_date = dto("1984-04-01")
    assert products.changedate([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 92


def test_seglength():
    assert products.seglength([{'start_day': dto("1986-04-01"),
                                'end_day': dto("1988-07-01")}],
                              dto("1984-04-01"),
                              bot=dto("1982-01-01")) == 821


def test_qa():
    assert products.curveqa([{'start_day': dto("1986-04-01"),
                              'end_day': dto("1988-07-01"),
                              'qa': 999}],
                             dto('1987-01-01')) == 999


#def test_run():
#    assert products.run('all', simplified_detect_results, dto("1980-01-01")) is True
