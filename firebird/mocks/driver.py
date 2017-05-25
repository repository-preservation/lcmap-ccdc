from firebird.mocks import aardvark as ma
from firebird import aardvark as a
from firebird import chip
from functools import partial
from firebird.driver import chip_spec_urls, csort, to_rod, to_pyccd, pyccd_dates


def pyccd_rdd(specs_url, chips_url, x, y, acquired):
    """
    clone of the driver modules pyccd_rdd module, replacing aardvark calls
    with methods pulling test data locally instead of from an http service
    """
    # create a partial function initialized with x, y and acquired since
    # those are static for this call to pyccd_rdd
    pchips = partial(ma.chips, x=x, y=y, acquired=acquired)
    # get all the specs, ubids, chips, intersecting dates and rods
    # keep track of the spectra they are associated with via dict key 'k'
    specs = {k: ma.chip_specs(v) for k, v in chip_spec_urls(specs_url).items()}
    ubids = {k: a.ubids(v) for k, v in specs.items()}
    chips = {k: csort(pchips(url=chips_url, ubids=u)) for k, u in ubids.items()}
    dates = a.intersection(map(a.dates, [c for c in chips.values()]))
    bspecs = specs['blues']
    locs = chip.locations(*chip.snap(x, y, bspecs[0]), bspecs[0]) # first is ok
    add_loc = partial(a.locrods, locs)
    rods = {k: add_loc(to_rod(v, dates, specs[k])) for k, v in chips.items()}
    del chips
    # for testing purposes, we dont want, or need, to run ccd.detect on 10k rods.
    # just return 1
    full_chip = to_pyccd(rods, pyccd_dates(dates))
    return [full_chip[0]]