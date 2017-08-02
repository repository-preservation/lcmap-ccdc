def specs(name):
    pass


def tif_to_json(tifpath, jsonpath, nodata=0):
    """Converts TIF files to JSON suitable for reading as DataFrames
    :param tifpath: Full path to input tif file
    :param jsonpath: Full path to output json file
    :param nodata: Nodata value.  Used to exclude values which shouldnt be
                   included in the conversion.
    :return: Full path of output json file
    """
    pass


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
