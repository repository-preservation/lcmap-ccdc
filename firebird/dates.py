from dateutil import parser

import firebird as fb

import re


def to_ordinal(datestring):
    '''
    Extract an ordinal date from a date string
    :param datestring: String with date value
    :return: An ordinal date
    '''
    return parser.parse(datestring).toordinal()


def startdate(acquired):
    '''
    Returns the startdate from an acquired date string
    :param acquired: / seperated date range in iso8601 format
    :return: Start date as string
    '''
    return acquired.split('/')[0]


def enddate(acquired):
    '''
    Returns the enddate from an acquired date string
    :param acquired: / seperated date range in iso8601 format
    :return: End date as string
    '''
    return acquired.split('/')[1]


def is_acquired(acquired):
    '''
    Is the date string a / seperated date range in iso8601 format?
    :param acquired: A date string
    :return: Boolean
    '''
    # 1980-01-01/2015-12-31
    regex = '^[0-9]{4}-[0-9]{2}-[0-9]{2}\/[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    return bool(re.match(regex, acquired))


def sort(datestrings):
    """ Sorts sequences of datestrings.
    :param datestrings: A sequence of date strings
    :returns: A sequence of formatted and sorted ordinal dates
    """
    return fb.sort([to_ordinal(d) for d in datestrings])


def rsort(datestrings):
    """ Reverse sorts sequences of datestrings.
    :param datestrings: A sequence of date strings
    :returns: A sequence of formatted and reverse sorted ordinal dates
    """
    return fb.rsort([to_ordinal(d) for d in datestrings])
