import ccd

def schema():
    pass

def inputs(timeseries):
    chip_x = timeseries[0][0]
    chip_y = timeseries[0][1]
    x = timeseries[0][2]
    y = timeseries[0][3]
    acquired = timeseries[0][5]
    data = timeseries[1] or dict()

    return {'chip_x'   : chip_x,
            'chip_y'   : chip_y,
            'x'        : x,
            'y'        : y,
            'acquired' : acquired,
            'data': {'dates'   : data.get('dates'),
                     'blues'   : data.get('blues'),
                     'greens'  : data.get('greens'),
                     'reds'    : data.get('reds'),
                     'nirs'    : data.get('nirs'),
                     'swir1s'  : data.get('swir1s'),
                     'swir2s'  : data.get('swir2s'),
                     'thermals': data.get('thermals'),
                     'qas'     : data.get('qas')}}

def dataframe(sc, rdd):
    pass

def read():
    pass

def write(sc, dataframe):
    pass

def execute(sc, timeseries):
    pass
