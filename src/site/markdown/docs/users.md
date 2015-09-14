title: User Guide

# Terminology

A variety of terms are used to describe time series data, and many of these apply to conflicting or
overlapping concepts.  In the interest of clarity, in spark-timeseries, we stick to the following
set of definitions:

* **Time Series** - A sequence of floating point values, each linked to a timestamp.
  In particular, we try as hard as possible to stick with “time series” as meaning a
  univariate time series, although in other contexts it sometimes refers to series with multiple
  values at the same timestamp.  In Scala, a time series is usually represented by a Breeze
  vector, and in Python, a 1-D numpy array, and has a DateTimeIndex somewhere nearby to link its
  values to points in time.
* **Key** - A string label used to identify a time series.  A TimeSeriesRDD is a distributed
  collection of tuples of (key, time series)
* **Instant** - The set of values in a collection of time series corresponding to a single point in
  time.
* **Observation** - a tuple of (timestamp, key, value)


