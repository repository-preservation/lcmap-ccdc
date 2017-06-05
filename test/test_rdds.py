from collections import namedtuple
from firebird import driver
from firebird import rdds
from test.mocks import aardvark as ma


def test_result_to_models():
    ChangeModel = namedtuple('ChangeModel', ['start_day', 'end_day'])
    m = ChangeModel(start_day='1980-01-01', end_day='2017-06-05')
    result = rdds.result_to_models({'change_models': [m]})
    assert isinstance(result, list)
    assert 'start_day' in result[0]
    assert 'end_day' in result[0]

'''
def test_ccd():
    inputs = driver.pyccd_inputs(point=(-100200, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    results = products.ccd((inputs[0][0], inputs[0][1]))
    assert type(results[0]) == tuple
    assert type(results[1]) == dict
    assert len(results[0]) == 3


def test_ccd_exception():
    inputs = driver.pyccd_inputs(point=(-100200, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    band_dict = inputs[0][1]
    band_dict.pop('reds')
    try:
        results = products.ccd(((111111, 222222), band_dict))
    except Exception as e:
        assert e is not None
'''
