# flags

def acquired():
    pass

def bounds():
    pass

def products():
    pass

def product_dates():
    pass

def clip():
    pass

# inputs

def initial(cfg, acquired, bounds, cache, cassandra, parquet, product_dates, clip):
    """Construct initial data

    Args:
        cfg (dict): System configuration and environment
        acquired (str): iso8601 date range
        bounds (tuple): tuple of tuple of points.  ((x1, y1), (x1, y2) ...)
        cache (tuple): sequence (str), transformations to persist
        cassandra (tuple): sequence (str), transformations to save
        parquet (tuple): sequence (str), transformations to save
        product_dates (tuple): dates for requested products
        clip (bool): True -> clip data to bounds False -> do not clip data to bounds.

    Returns:
        dict: cfg, acquired, bounds, cache, cassandra, parquet, product_dates, clip
    """
    
    return {'cfg': cfg,
            'acquired': acquired,
            'bounds': bounds,
            'cache': cache,
            'cassandra': cassandra,
            'parquet': parquet,
            'product_dates': product_dates,
            'clip': clip}


def time_series(initial):
    """Construct time_series data
    
    Args:
        initial (dict): cfg, acquired, bounds, clip

    Returns:
        rdd: sequence of dicts 
    """
    # ts = merlin.create()
    # cassandra(initial, ts) if 'time_series' in initial['cassandra']
    # parquet(initial, ts) if 'time_series' in initial['parquet']
    # persist(ts) if 'time_series' in initial['cache']
    # return ts
    pass


# I don't like this as much as I do having separate functions for each
def ancillary(initial):
    """Retrieve ancillary data

    Args:
        initial (dict): cfg, acquired, bounds, cache, cassandra, parquet, product_dates, clip

    Returns:
        dict: ancillary data merged with initial
    """
    pass


def aspect(initial):
    """Retrieve aspect data

    Args:
        initial (dict): cfg, acquired, bounds, clip

    Returns:
        rdd: sequence of dicts
    """
    # return merlin.create()
    pass

def dem(initial):
    # return merlin.create()
    pass

def mpw(initial):
    # return merlin.create()
    pass

def posidex(initial):
    # return merlin.create()
    pass

def trends(initial):
    # return merlin.create()
    pass

# outputs

def change_detection(data, time_series):
    pass

def training(cfg, change_detection, aspect, dem, mpw, posidex, trends):
    pass

def classification(cfg, change_detection, training):
    pass

def seglength(cfg, change_detection, product_dates):
    pass

def curveqa(cfg, change_detection, product_dates):
    pass

def changemag(cfg, change_detection, product_dates):
    pass

def lastchange(cfg, change_detection, product_dates):
    pass

# storage

def cache(cfg, output):
    pass

def cassandra(cfg, output):
    pass

def parquet(cfg, directory, output):
    pass

def save(name, data, initial):

    #if name in initial['cache']:
    #    cache()
        
    #if name in initial['cassandra']:
    #    cassandra()

    #if name in initial['parquet']:
    #    parquet()

    pass
    
# conversion

def raster(data):
    pass

def dataframe(rdd):
    pass

def rdd(dataframe):
    pass

# formatting

def success():
    pass

def error():
    pass
