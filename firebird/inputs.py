from firebird import aardvark as a
from firebird import chip
from firebird import dates as fdates
from functools import partial
import firebird as fb


def to_rod(chips, dates, specs):
    """ Function to convert chips to rods.
        Exists primarily to clean up syntax """
    return a.rods(a.to_numpy(a.trim(chips, dates), a.byubid(specs)))


def to_pyccd(located_rods_by_spectra, dates):
    """ Organizes rods by xy instead of by spectrum
    :param located_rods_by_spectra: dict of dicts, keyed first by spectra
                                    then by coordinate
    :returns: List of tuples
    :example:
    located_rods_by_spectra parameter:
    {'red':   {(0, 0): [110, 110, 234, 664], (0, 1): [23, 887, 110, 111]}}
    {'green': {(0, 0): [120, 112, 224, 624], (0, 1): [33, 387, 310, 511]}}
    {'blue':  {(0, 0): [128, 412, 244, 654], (0, 1): [73, 987, 119, 191]}}
    ...

    returns:
    (((0, 0), {'red':   [110, 110, 234, 664],
               'green': [120, 112, 224, 624],
               'blue':  [128, 412, 244, 654], ... }),
     ((0, 1), {'red':   [23, 887, 110, 111],
               'green': [33, 387, 310, 511],
               'blue':  [73, 987, 119, 191], ...}))
    ...
    """
    def colors(spectra, rods, xy):
        return {spec: rods[spec][xy] for spec in spectra}

    def add_dates(rainbow, dates):
        rainbow['dates'] = dates
        return rainbow

    # alias the descriptive name down to something that doesn't take up a line
    locrods   = located_rods_by_spectra
    spectra   = tuple(locrods.keys())
    locations = locrods[spectra[0]].keys()
    rainbow   = partial(colors, spectra=locrods.keys(), rods=locrods)
    return tuple([(xy, add_dates(rainbow(xy=xy), dates)) for xy in locations])


def sort(chips):
    """
    Sorts all the returned chips by date.
    :param chips: sequence of chips
    :returns: sorted sequence of chips
    """
    return tuple(fb.rsort(chips, key=lambda c: c['acquired']))


def pyccd(point, specs_url, specs_fn, chips_url, chips_fn, acquired, queries):
    """
    :param point: A tuple of (x, y) which is within the extents of a chip
    :param specs_url: URL to the chip specs host:port/context
    :param specs_fn:  Function that accepts a url query and returns chip specs
    :param chips_url: URL to the chips host:port/context
    :param chips_fn:  Function that accepts x, y, acquired, url, ubids and
                      returns chips.
    :param acquired: Date range string as start/end, ISO 8601 date format
    :param queries: dict of URL queries to retrieve chip specs keyed by spectra
    :returns: A tuple of tuples.
    (((x1, y1), {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []}),
     ((x1, y2), {'dates': [],  'reds': [],     'greens': [],
                 'blues': [],  'nirs1': [],    'swir1s': [],
                 'swir2s': [], 'thermals': [], 'quality': []}))
    """
    # create a partial function initialized with x, y and acquired since
    # those are static for this call to pyccd_rdd
    #pchips = partial(a.chips, x=point[0], y=point[1], acquired=acquired)
    pchips = partial(chips_fn, x=point[0], y=point[1], acquired=acquired)

    # get all the specs, ubids, chips, intersecting dates and rods
    # keep track of the spectra they are associated with via dict key 'k'
    #specs = {k: a.chip_specs(v) for k, v in chip_spec_urls(specs_url).items()}
    specs = {k: specs_fn(v) for k, v in queries.items()}

    ubids = {k: a.ubids(v) for k, v in specs.items()}

    chips = {k: sort(pchips(url=chips_url, ubids=u)) for k, u in ubids.items()}

    dates = a.intersection(map(a.dates, [c for c in chips.values()]))

    bspecs = specs['blues']

    locs = chip.locations(*chip.snap(*point, bspecs[0]), bspecs[0])

    add_loc = partial(a.locrods, locs)
    rods = {k: add_loc(to_rod(v, dates, specs[k])) for k, v in chips.items()}
    del chips

    return to_pyccd(rods, fdates.rsort(dates))
