from firebird.io import cassandra
from firebird import results
from pyspark.sql.types import StringType
import firebird
import merlin


def spec_queries(url):
    """A map of pyccd spectra to chip-spec queries

    Args:
        url (str): full url (http://host:port/context) for chip-spec endpoint

    Returns:
        dict: spectra to chip spec queries

    Example:
        >>> chip_spec_queries('http://host/v1/landsat/chip-specs')
        {'reds':     'http://host/v1/landsat/chip-specs?q=tags:red AND sr',
         'greens':   'http://host/v1/landsat/chip-specs?q=tags:green AND sr'
         'blues':    'http://host/v1/landsat/chip-specs?q=tags:blue AND sr'
         'nirs':     'http://host/v1/landsat/chip-specs?q=tags:nir AND sr'
         'swir1s':   'http://host/v1/landsat/chip-specs?q=tags:swir1 AND sr'
         'swir2s':   'http://host/v1/landsat/chip-specs?q=tags:swir2 AND sr'
         'thermals': 'http://host/v1/landsat/chip-specs?q=tags:thermal AND ta'
         'quality':  'http://host/v1/landsat/chip-specs?q=tags:pixelqa'}
    """

    return {'reds':     ''.join([url, '?q=tags:red AND sr']),
            'greens':   ''.join([url, '?q=tags:green AND sr']),
            'blues':    ''.join([url, '?q=tags:blue AND sr']),
            'nirs':     ''.join([url, '?q=tags:nir AND sr']),
            'swir1s':   ''.join([url, '?q=tags:swir1 AND sr']),
            'swir2s':   ''.join([url, '?q=tags:swir2 AND sr']),
            'thermals': ''.join([url, '?q=tags:bt AND thermal AND NOT tirs2']),
            'quality':  ''.join([url, '?q=tags:pixelqa'])}


def read(chip_x, chip_y, acquired=None):
    # make sure to use predicate pushdown.
    # otherwise youll read every record in cassandra.
    # https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md
    
    # df = cassandra.read('timeseries').load()

    # if acquired is None:
    #     return df.filter(df.chip_x == chip_x, df.chip_y == chip_y)
    # else:
    #     return df.filter(df.chip_x == chip_x, df.chip_y == chip_y, df.datestr == acquired)
    raise NotImplemented
  
def write(rdd, jobconf):
    #return cassandra.write('timeseries',
    #                       dataframe(rdd, results.schema(StringType()))
    raise NotImplemented


def execute(acquired, bounds, refspec, chips_fn=None, specs_fn=None):
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
                                 specs_fn=specs_fn)
