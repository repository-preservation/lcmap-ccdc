from firebird import chips as fchips
from firebird import chip_specs as fspecs
from firebird import dates as fdates
from firebird import functions as f
from firebird import rods as frods
import firebird as fb


def sort(chips, key=lambda c: c['acquired']):
    """Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(f.rsort(chips, key=key))


def add_dates(dates, dods, key='dates'):
    """Inserts dates into each subdictionary of the parent dictionary.
    :param dod: A dictionary of dictionaries
    :param dates: A sequence of dates
    :param key: Subdict key where dates values is inserted
    :return: An updated dictionary of dictionaries with
    """
    def update(d, v):
        d.update({key: v})
        return d
    return {k: update(v, dates) for k, v in d.items()}


def identify(chip_x, chip_y, rod):
    """Adds chip ids (chip_x, chip_y) to the key for each dict entry
    :param chip_x: x coordinate that identifies the source chip
    :param chip_y: y coordinate that identifies the source chip
    :param rod: dict of (x, y): [values]
    :return: dict of (chip_x, chip_y, x, y): [values]
    """
    return {(chip_x, chip_y, k[0], k[1]): v for k, v in rod.items()}


def pyccd(point, specs_url, specs_fn, chips_url, chips_fn, acquired, queries):
    """Builds inputs for the pyccd algorithm.
    :param point: A tuple of (x, y) which is within the extents of a chip
    :param specs_url: URL to the chip specs host:port/context
    :param specs_fn:  Function that accepts a url query and returns chip specs
    :param chips_url: URL to the chips host:port/context
    :param chips_fn:  Function that accepts x, y, acquired, url, ubids and
                      returns chips.
    :param acquired: Date range string as start/end, ISO 8601 date format
    :param queries: dict of URL queries to retrieve chip specs keyed by spectra
    :returns: A tuple of tuples.
    (((chip_x, chip_y, x1, y1), {'dates': [],  'reds': [],     'greens': [],
                                 'blues': [],  'nirs1': [],    'swir1s': [],
                                 'swir2s': [], 'thermals': [], 'quality': []}),
     ((chip_x, chip_y, x1, y2), {'dates': [],  'reds': [],     'greens': [],
                                 'blues': [],  'nirs1': [],    'swir1s': [],
                                 'swir2s': [], 'thermals': [], 'quality': []}))
    """

    # get all the specs, chips, intersecting dates and rods
    # keep track of the spectra they are associated with via dict key 'k'
    specs = {k: specs_fn(v) for k, v in queries.items()}

    chips = {k: sort(chips_fn(
                         x=point[0],
                         y=point[1],
                         acquired=acquired,
                         url=chips_url,
                         ubids=fspecs.ubids(v)))
            for k, v in specs.items()}

    dstrs = f.intersection(map(fchips.dates, [c for c in chips.values()]))

    blue_chip_spec = specs['blues'][0]
    chip_x, chip_y = fchips.snap(*point, blue_chip_spec)
    chip_locations = fchips.locations(chip_x, chip_y, blue_chip_spec)

    # LETS MAKE SOME RODS :-)  (life (is (way better) (with s-expressions)))
    rods = {k: add_dates(
                   f.rsort(map(fdates.to_ordinal, dstrs)),
                   f.flip_keys(
                       identify(
                           chip_x,
                           chip_y,
                           frods.locate(
                               chip_locations,
                               frods.from_chips(
                                   fchips.to_numpy(
                                       fchips.trim(v, dstrs),
                                       fspecs.byubid(specs[k]))))))
            for k, v in chips.items()}

    # convert to tuple and return
    return tuple((k, v) for k, v in rods.items())
