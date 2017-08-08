from cytoolz.curried import compose, filter, identity, map, merge, partial, pipe
from operator import add, mul
from osgeo import gdal
import functools


def location(spec, yx):
    """Determines projection space location from array positions + spec
    :param spec: dict containing ulx, uly, pixel_x, pixel_y
    :param yx: sequence of y array position, x array position
    :return: dict of location keyed with y_index, x_index, y, x, spec
    """
    return merge([spec, {'y_index': yx[0],
                         'x_index': yx[1],
                         'y': spec['uly'] + (yx[0] * spec['pixel_y']),
                         'x': spec['ulx'] + (yx[1] * spec['pixel_x'])}])


def locations(spec):
    """Generator for all locations represented by a spec
    :param spec: dict containing xsize, ysize, pixel_x, pixel_y, ulx, uly keys
    :return: Generator yielding spec plus x, y, x_index and y_index keys
    """
    locator = partial(location, spec=spec)
    indices = ((y, x) for x in range(spec['xsize'])
                      for y in range(spec['ysize']))
    return map(locator, indices)


@functools.lru_cache(maxsize=128, typed=True)
def genspec(file, nodata=None, fill=None):
    """Creates spec data structure from a gdal supported raster file
    :param file: Full path to a raster file
    :return: dict of spec values
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


def value(memarray, spec):
    """Extracts a value from a memory array
    :param memarray: two dimensional data array
    :param spec: dict with y and x keys
    :return:  Value associated with y and x array positions
    """
    return memarray[spec['y']][spec['x']]


def compact(spec):
    """Excludes nodata and fill values from the dataset
    :param spec: A spec with value, nodata and fill keys
    :return: A filtered spec excluding nodata and fill
    """
    return partial(filter, lambda spec: spec['value'] not in (spec['nodata'],
                                                              spec['fill']))


def read(filepath, locations):
    """Reads values from file specified by locations
    :param filepath: Path to raster to read from
    :param locations: sequence of dicts containing y_index, x_index, y, x
    :return: lazy sequence of merged dicts with 'value' key populated
    """
    ds = None
    try:
        ds = gdal.Open(filepath)
        ma = ds.GetRasterBand(1).GetVirtualMemArray()
        reader = merge(juxt(identity, lambda spec: {'value': value(ma, spec)}))
        return map(reader, locations)
    finally:
        ds = None


def csv(specs):
    """Converts a spec dict to a csv entry
    :param specs: Sequence of spec dicts
    :return: Sequence of csv records ready to be written
    """
    return map(lambda spec: '{x},{y},{value}\n'.format(**spec), specs)


def write(filepath, values):
    """Creates a new filewriter for the file named by filepath
    :param filepath: Path, name and extension of file to write to
    :param values: Sequence of values to write
    :return: Sequence of results from handle.write(value) function calls
    """
    with open(filepath, 'w+') as handle:
        return map(handle.write(value), values)


def tif_to_json(tifpath, outpath):
    """Converts TIF files to JSON suitable for reading as DataFrames
    :param tifpath: Full path to input tif file
    :param jsonpath: Full path to output json file
    :param nodata: Nodata value.  Used to exclude values which shouldnt be
                   included in the conversion.
    :return: Full path of output json file
    """
    filewriter = writer(outpath)

    return pipe(tifpath,
                genspec,
                locations,
                partial(read, filepath=tifpath),
                compact,
                csv,
                partial(write, filepath=outpath))


def ingest(jsonpath, spec, table=None, bbox=None):
    """Ingests ancillary data located at path, specified by spec and constrained
    by bbox.
    :param path: Path to the json ancillary file
    :param spec: spec for ancillary data containing pixel size, no data
    :param name: Name to use for DataFrame table.  If None, derive from jsonpath
                 filename.
    :param bbox: dict() keyed by ulx, uly, lrx, lry.  Only ingest data points
                 that fit within bbox.  If None, ingest all
    :return: Tuple of (num_points_ingested, num_points_excluded)
    """
    pass
