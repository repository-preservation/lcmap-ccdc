from firebird import functions as f
from test import data

index = data.spectra_index(data.test_specs())

def chips(x, y, acquired, url, ubids):
    spectra = set(index[ubid] for ubid in ubids)
    return tuple(f.flatten([data.chips(s) for s in spectra]))

def chip_specs(url):
    spectra = set(data.spectra_from_queryid(data.spec_query_id(url)))
    return tuple(f.flatten([data.chip_specs(s) for s in spectra]))
