from __future__ import print_function

__author__ = 'dneise, mnoethe'

""" This file contains some functions to deal with FACT modified modified julian date

The time used most of the time in FACT is the number of days since 01.01.1970

So this time is related to unix time, since it has the same offset
(unix time is the number of seconds since 01.01.1970 00:00:00)
but it is also related to "the" Modified Julian Date (MJD),
which is used by astronomers
in the sense, that it also counts days.

According to http://en.wikipedia.org/wiki/Julian_day,
there is quite a large number of
somehow modified julian dates, of which the MJD is only one.
So it might be okay, to introduce a new modification,
going by the name of FACT Julian Date (FJD).
"""

import time
import calendar
from datetime import datetime
import logging

import dateutil
import dateutil.parser

OFFSET = (datetime(1970, 1, 1) - datetime(1, 1, 1)).days


def fjd(datetime_inst):
    """ convert datetime instance to FJD
    """
    if datetime_inst.tzinfo is None:
        logging.warning("datetime instance is not aware of its timezone."
                        " Result possibly wrong!")
    return calendar.timegm(datetime_inst.utctimetuple()) / (24.*3600.)


def iso2dt(iso_time_string):
    """ parse ISO time string to timezone aware datetime instance

    example
    2015-01-23T08:08+01:00
    """
    datetime_inst = dateutil.parser.parse(iso_time_string)
    # make aware at any cost!
    if datetime_inst.tzinfo is None:
        print("ISO time string did not contain timezone info. I assume UTC!")
        datetime_inst = datetime_inst.replace(tzinfo=dateutil.tz.tzutc())
    return datetime_inst


def run2dt(run_string):
    """ parse typical FACT run file path string to datetime instance (UTC)

    example
    first you do this:

    "/path/to/file/20141231.more_text" --> "20141231"
    then call
    run2dt("20141231")
    """
    format_ = "%Y%m%d"
    datetime_inst = datetime.strptime(run_string, format_)
    datetime_inst = datetime_inst.replace(tzinfo=dateutil.tz.tzutc())
    return datetime_inst


def facttime(time_string):
    """ conver time-string with format %Y%m%d %H:%M  to fact time
    """
    return calendar.timegm(time.strptime(
        time_string, "%Y%m%d %H:%M")) / (24.*3600.)


def to_datetime(fact_julian_date):
    """ convert facttime to datetime instance
    """
    unix_time = fact_julian_date*24*3600
    datetime_inst = datetime.utcfromtimestamp(unix_time)
    return datetime_inst


def datestr(datetime_inst):
    """ make iso time string from datetime instance
    """
    return datetime_inst.isoformat("T")
