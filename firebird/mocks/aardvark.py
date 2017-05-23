from test import fixtures


def chips(x, y, acquired, url, ubids):
    if 'LANDSAT_4/TM/sr_band3' in ubids:
        _c = 'red'
    elif 'LANDSAT_4/TM/sr_band4' in ubids:
        _c = 'nir'
    elif 'LANDSAT_4/TM/sr_band7' in ubids:
        _c = 'swir2'
    elif 'LANDSAT_4/TM/cfmask' in ubids:
        _c = 'cfmask'
    elif 'LANDSAT_4/TM/sr_band2' in ubids:
        _c = 'green'
    elif 'LANDSAT_4/TM/sr_band5' in ubids:
        _c = 'swir1'
    elif 'LANDSAT_4/TM/sr_band1' in ubids:
        _c = 'blue'
    else:
        _c = 'thermal'
    return list(fixtures.chips(_c))


def chip_specs(url):
    if 'pixelqa' in url:
        _i = 'cfmask'
    elif 'thermal' in url:
        _i = 'thermal'
    elif 'nir' in url:
        _i = 'nir'
    elif 'red' in url:
        _i = 'red'
    elif 'green' in url:
        _i = 'green'
    elif 'swir1' in url:
        _i = 'swir1'
    elif 'swir2' in url:
        _i = 'swir2'
    else:
        _i = 'blue'
    # currently no chip data for L8, so don't grab specs
    return [i for i in fixtures.chip_specs(_i) if "LANDSAT_8" not in i['ubid']]

