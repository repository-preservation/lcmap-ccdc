from cytoolz import filter, identity, juxt, map, merge, partial, pipe, reduce
from operator import add, mul
from osgeo import gdal
import functools


def locate(yx, spec):
    """Determines projection space location from array positions + spec
    :param spec: dict containing ulx, uly, pixel_x, pixel_y
    :param yx: sequence of y array position, x array position
    :return: dict of location keyed with y_index, x_index, y, x, spec
    """
    return merge([spec, {'y_index': yx[0],
                         'x_index': yx[1],
                         'y': int(spec['uly'] + (yx[0] * spec['pixel_y'])),
                         'x': int(spec['ulx'] + (yx[1] * spec['pixel_x']))}])


def locations(spec):
    """Generator for all locations represented by a spec
    :param spec: dict containing xsize, ysize, pixel_x, pixel_y, ulx, uly keys
    :return: Generator yielding spec plus x, y, x_index and y_index keys
    """
    locator = partial(locate, spec=spec)
    indices = ((y, x) for y in range(spec['ysize'])
                      for x in range(spec['xsize']))
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


def value(spec, memarray):
    """Extracts a value from a memory array
    :param spec: dict with y and x keys
    :param memarray: two dimensional data array
    :return:  Value associated with y and x array positions
    """
    return memarray[spec['y_index']][spec['x_index']]


def compact(specs):
    """Excludes nodata and fill values from the dataset
    :param spec: A spec with value, nodata and fill keys
    :return: A filtered spec excluding nodata and fill
    """
    return filter(lambda s: s['value'] not in (s['nodata'], s['fill']), specs)


def read(specs, filepath):
    """Reads values from file specified by locations
    :param filepath: Path to raster to read from
    :param specs: sequence of dicts containing y_index, x_index, y, x
    :return: Generated sequence of merged spec dicts with 'value' key populated
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
    :param specs: Sequence of spec dicts
    :return: Sequence of csv records ready to be written
    """
    return map(lambda spec: '{x},{y},{value}\n'.format(**spec), specs)


def write(values, filepath):
    """Writes a sequence of values to a filepath
    :param filepath: Path, name and extension of file to write to
    :param values: Sequence of values to write
    :return: Sequence of results from handle.write(value) function calls
    """
    with open(filepath, 'w+') as handle:
        return (filepath, reduce(add, map(handle.write, values)))


def tif_to_json(tifpath, outpath, nodata=None, fill=None, partition_size=10000):
    """Converts TIF files to JSON suitable for reading as DataFrames
    :param tifpath: Full path to input tif file
    :param jsonpath: Full path to output json file
    :param fill: Fill value.  Used to exclude values from the conversion
    :param nodata: Nodata value.  Used to exclude values which shouldnt be
                   included in the conversion.
    :param partition_size: How big should each file partition be
    :return: Full path of output json file
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
    :param path: Path to the json ancillary file
    :param spec: spec for ancillary data containing pixel size, nodata, fill
    :param name: Name to use for DataFrame table.  If None, derive from jsonpath
                 filename.
    :param bbox: dict() keyed by ulx, uly, lrx, lry.  Only ingest data points
                 that fit within bbox.  If None, ingest all
    :return: Tuple of (num_points_ingested, num_points_excluded)
    """
    pass
