from firebird.io import cassandra
from firebird import results
from pyspark.sql.types import StringType
import firebird


def spec_queries(url=firebird.SPECS_URL):
    """A map of aux data to spec queries

    Args:
        url (str): full url (http://host:port/context) for chip-spec endpoint

    Returns:
        dict: aux data to chip spec queries

    Example:
        >>> queries('http://host/v1/landsat/chip-specs')
        {'dem':     'http://host/v1/landsat/chip-specs?q=tags:dem',
         'trends':  'http://host/v1/landsat/chip-specs?q=tags:trends'
         'mpw':     'http://host/v1/landsat/chip-specs?q=tags:mpw'
         'aspect':  'http://host/v1/landsat/chip-specs?q=tags:aspect'
         'posidex': 'http://host/v1/landsat/chip-specs?q=tags:posidex'}
    """

    return {'dems':    ''.join([url, '?q=tags:dem']),
            'trends':  ''.join([url, '?q=tags:trends']),
            'mpw':     ''.join([url, '?q=tags:mpw']),
            'aspect':  ''.join([url, '?q=tags:aspect']),
            'posidex': ''.join([url, '?q=tags:posidex'])}


def execute(acquired, bounds, chips_fn=firebird.chips_fn, specs_fn=firebird.specs_fn):
    """Returns a timeseries of aux data.

    Args:
        acquired (str): iso8601 date range
        bounds (sequence(sequence)): Sequence of (x,y)
        chips_fn (func): Function that will return chips given point, acquired and ubids
        specs_fn (func): Function that will return specs given a spec query

    Returns:
        pyspark.sql.dataframe.DataFrame: timeseries of aux data 'k1': [data,], 'k2': [data,] ...}
    """

    # get or create context
    # compute chip ids from bounds,
    # parallelize chip ids, configure parallelism
    # run merlin on configured spectra
    # return the rdd

    coordinates = merlin.chips.bounds_to_coordinates(bounds, refspec)
    
    rdd = firebird.spark_session().sparkContext.parallelize(coordinates,
                                                            firebird.INITIAL_PARTITION_COUNT)
          
    return rdd.map(merlin.create(acquired=acquired,
                                 chips_fn=chips_fn,
                                 specs_fn=specs_fn,
                                 queries=spec_queries(),
                                 chips_url=firebird.CHIPS_URL)
