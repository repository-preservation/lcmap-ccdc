from cytoolz import filter, identity, juxt, map, merge, partial, pipe, reduce
from operator import add, mul
from osgeo import gdal
import functools


def locate(yx, spec):
    """Determines projection space location from array positions + spec

    Args:
        spec (dict): ulx, uly, pixel_x, pixel_y
        yx (sequence): y array position, x array position

    Returns:
        dict: location keyed with y_index, x_index, y, x, spec
    """

    return merge([spec, {'y_index': yx[0],
                         'x_index': yx[1],
                         'y': int(spec['uly'] + (yx[0] * spec['pixel_y'])),
                         'x': int(spec['ulx'] + (yx[1] * spec['pixel_x']))}])


def locations(spec):
    """Generator for all locations represented by a spec

    Args:
        spec (dict): xsize, ysize, pixel_x, pixel_y, ulx, uly keys

    Returns:
        Generator yielding spec plus x, y, x_index and y_index keys
    """

    locator = partial(locate, spec=spec)
    indices = ((y, x) for y in range(spec['ysize'])
                      for x in range(spec['xsize']))
    return map(locator, indices)


@functools.lru_cache(maxsize=128, typed=True)
def genspec(file, nodata=None, fill=None):
    """Creates spec data structure from a gdal supported raster file

    Args:
        file (str): Full path to a raster file

    Returns:
        dict: spec values
    """

    ds = None
    try:
        ds = gdal.Open(file)
        transform = ds.GetGeoTransform()
        return {
            'xsize': ds.RasterXSize,
            'ysize': ds.RasterYSize,
            'pixel_x': transform[1],
            'pixel_y': transform[5],
            'nodata': None,
            'fill': None,
            'ulx': transform[0],
            'uly': transform[3],
            'wkt': ds.GetProjection(),
        }
    finally:
        ds = None


def value(spec, memarray):
    """Extracts a value from a memory array
    Args:
        spec (dict): y and x keys
        memarray: two dimensional data array

    Returns:
        Value associated with y and x array positions
    """

    return memarray[spec['y_index']][spec['x_index']]


def compact(specs):
    """Excludes nodata and fill values from the dataset

    Args:
        specs (dict): A spec with value, nodata and fill keys

    Returns:
        Filtered specs excluding nodata and fill
    """

    return filter(lambda s: s['value'] not in (s['nodata'], s['fill']), specs)


def read(specs, filepath):
    """Reads values from file specified by locations

    Args:
        filepath (str): Full path of raster file to read
        specs (sequence): dicts containing y_index, x_index, y, x

    Returns:
        sequence: dicts with 'value' key populated
    """

    ds = None
    try:
        ds = gdal.Open(filepath)
        val = partial(value, memarray=ds.GetRasterBand(1).GetVirtualMemArray())
        reader = juxt([identity, lambda spec: dict(value=val(spec=spec))])
        for spec in specs:
            yield (merge(reader(spec)))
    finally:
        ds = None


def csv(specs):
    """Converts a spec dict to a csv entry

    Args:
        specs (sequence): spec dicts

    Returns:
        sequence: csv records
    """

    return map(lambda spec: '{x},{y},{value}\n'.format(**spec), specs)


def write(values, filepath):
    """Writes a sequence of values to a filepath

    Args:
        filepath (str): Full path of raster file to write
        values (sequence): Values to write

    Returns:
        tuple: (filepath, bytes written)
    """

    with open(filepath, 'w+') as handle:
        return (filepath, reduce(add, map(handle.write, values)))


def tif_to_json(tifpath, outpath, nodata=None, fill=None, partition_size=10000):
    """Converts TIF files to JSON suitable for reading as DataFrames

    Args:
        tifpath (str): Full path to input tif file
        jsonpath (str): Full path to output json file
        fill: Fill value.  Used to exclude values from the conversion
        nodata: Nodata value.  Used to exclude values which shouldnt be
                included in the conversion.
        partition_size (int): How big should each file partition be

    Returns:
        str: Full path of output json file
    """

    reader = partial(read, filepath=tifpath)
    writer = partial(write, filepath=outpath)

    return pipe(tifpath,
                genspec,
                locations,
                reader,
                compact,
                csv,
                writer)


def ingest(csvpath, spec, table=None, bbox=None):
    """Ingests ancillary data located at path, specified by spec and constrained
    by bbox.

    Args:
        path (str): Path to the json ancillary file
        spec (dict): ancillary data spec containing pixel size, nodata, fill
        name (str): Name to use for DataFrame table.  If None, derive from
                    jsonpath filename.
        bbox (dict): ulx, uly, lrx, lry.  Only ingest data points that fit
                     within bbox.  If None, ingest all

    Returns:
        tuple: (num_points_ingested, num_points_excluded)
    """
    pass
