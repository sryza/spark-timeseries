from py4j.java_gateway import java_import
from utils import datetime_to_millis
import numpy as np
import pandas as pd

class DateTimeIndex(object):
    """
    A DateTimeIndex maintains a bi-directional mapping between integers and an ordered collection of
    date-times. Multiple date-times may correspond to the same integer, implying multiple samples
    at the same date-time.

    To avoid confusion between the meaning of "index" as it appears in "DateTimeIndex" and "index"
    as a location in an array, in the context of this class, we use "location", or "loc", to refer
    to the latter.
    """ 

    def __init__(self, jdt_index):
        self._jdt_index = jdt_index

    def __len__(self):
        """Returns the number of timestamps included in the index."""
        return self._jdt_index.size()

    def first(self):
        """Returns the earliest timestamp in the index, as a Pandas Timestamp."""
        millis = self._jdt_index.first().getMillis()
        return pd.Timestamp(millis * 1000000)

    def last(self):
        """Returns the latest timestamp in the index, as a Pandas Timestamp."""
        millis = self._jdt_index.last().getMillis()
        return pd.Timestamp(millis * 1000000)

    def __getitem__(self, val):
        # TODO: throw an error if the step size is defined
        if isinstance(val, slice):
            start = datetime_to_millis(val.start)
            stop = datetime_to_millis(val.stop)
            jdt_index = self._jdt_index.slice(start, stop)
            return DateTimeIndex(jdt_index)
        else:
            return self._jdt_index.locAtDateTime(datetime_to_millis(val))

    def islice(self, start, end):
        """
        Returns a new DateTimeIndex, containing a subslice of the timestamps in this index,
        as specified by the given integer start and end locations.

        Parameters
        ----------
        start : int
            The location of the start of the range, inclusive.
        end : int
            The location of the end of the range, exclusive.
        """
        jdt_index = self._jdt_index.islice(start, end)
        return DateTimeIndex(jdt_index=jdt_index)

    def datetime_at_loc(self, loc):
        """Returns the timestamp at the given integer location as a Pandas Timestamp."""
        millis = self._jdt_index.dateTimeAtLoc(loc).getMillis()
        return pd.Timestamp(millis * 1000000)

    def to_pandas_index(self):
        """Returns a pandas.DatetimeIndex representing the same date-times"""
        # TODO: we can probably speed this up for uniform indices
        arr = self._jdt_index.toMillisArray()
        arr = [arr[i] * 1000000 for i in xrange(len(self))]
        return pd.DatetimeIndex(arr)

    def __eq__(self, other):
        return self._jdt_index.equals(other._jdt_index)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return self._jdt_index.toString()

class _Frequency(object):
    def __eq__(self, other):
        return self._jfreq.equals(other._jfreq)

    def __ne__(self, other):
       return not self.__eq__(other)

class DayFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in days.
    """
  
    def __init__(self, days, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.DayFrequency(days)

    def days(self):
        return self._jfreq.days()

class HourFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in hours.
    """

    def __init__(self, hours, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.HourFrequency(hours)

    def hours(self):
        return self_jfreq.hours()

class BusinessDayFrequency(object):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in
    business days.
    """

    def __init__(self, bdays, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.BusinessDayFrequency(bdays)

    def __eq__(self, other):
         return self._jfreq.equals(other._jfreq)

    def __ne__(self, other):
        return not self.__eq__(other)

def uniform(start, end=None, periods=None, freq=None, sc=None):
    """
    Instantiates a uniform DateTimeIndex.

    Either end or periods must be specified.
    
    Parameters
    ----------
        start : string, long (millis from epoch), or Pandas Timestamp
        end : string, long (millis from epoch), or Pandas Timestamp
        periods : int
        freq : a frequency object
        sc : SparkContext
    """
    dtmodule = sc._jvm.com.cloudera.sparkts.__getattr__('DateTimeIndex$').__getattr__('MODULE$')
    if freq is None:
        raise ValueError("Missing frequency")
    elif end is None and periods == None:
        raise ValueError("Need an end date or number of periods")
    elif end is not None:
        return DateTimeIndex(dtmodule.uniform( \
            datetime_to_millis(start), datetime_to_millis(end), freq._jfreq))
    else:
        return DateTimeIndex(dtmodule.uniform( \
            datetime_to_millis(start), periods, freq._jfreq))

def irregular(timestamps, sc):
    """
    Instantiates an irregular DateTimeIndex.
    
    Parameters
    ----------
        timestamps : a Pandas DateTimeIndex, or an array of strings, longs (nanos from epoch), Pandas
            Timestamps
        sc : SparkContext
    """
    dtmodule = sc._jvm.com.cloudera.sparkts.__getattr__('DateTimeIndex$').__getattr__('MODULE$')
    arr = sc._gateway.new_array(sc._jvm.long, len(timestamps))
    for i in xrange(len(timestamps)):
        arr[i] = datetime_to_millis(timestamps[i])
    return DateTimeIndex(dtmodule.irregular(arr))

