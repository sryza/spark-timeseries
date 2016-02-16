title: User Guide

# User Guide

## Terminology

A variety of terms are used to describe time series data, and many of these apply to conflicting or
overlapping concepts.  In the interest of clarity, we stick to the following
set of definitions:

* **Time Series** - A sequence of floating point values, each linked to a timestamp.
  In particular, we try to stick with “time series” as meaning a
  univariate time series, although in other contexts it sometimes refers to series with multiple
  values at the same timestamp.  A notable instance of the latter is the `TimeSeries` class, which
  refers to a multivariate time series. In Scala, a time series is usually represented by a Breeze
  vector, and in Python, a 1-D numpy array, and has a DateTimeIndex somewhere nearby to link its
  values to points in time.
* **Key** - A string label used to identify a time series.  A TimeSeriesRDD is a distributed
  collection of tuples of (key, time series)
* **Instant** - The set of values in a collection of time series corresponding to a single point in
  time.
* **Observation** - A tuple of (timestamp, key, value).


## Abstractions

### TimeSeriesRDD

The central abstraction of the library is the `TimeSeriesRDD`, a lazy distributed collection of
univariate series with a conformed time dimension. It is lazy in the sense that it is an RDD: it
encapsulates all the information needed to generate its elements, but doesn't materialize them upon
instantiation. It is distributed in the sense that different univariate series within the collection
can be stored and processed on different nodes.  Within each univariate series, observations are not
distributed. The time dimension is conformed in the sense that a single `DateTimeIndex` applies to
all the univariate series. Each univariate series within the RDD has a key to identify it. 

TimeSeriesRDDs then support efficient series-wise operations like slicing, imputing missing values
based on surrounding elements, and training time-series models.  For example, in Scala:

    val tsRdd: TimeSeriesRDD = ...
    
    // Find a sub-slice between two dates 
    val subslice = tsRdd.slice(new DateTime("2015-4-10"), new DateTime("2015-4-14"))
    
    // Fill in missing values based on linear interpolation
    val filled = subslice.fill("linear")
    
    // Use an AR(1) model to remove serial correlations
    val residuals = filled.mapSeries(series => ar(series, 1).removeTimeDependentEffects(series))

Or in Python:

    tsrdd = ...

    # Find a sub-slice between two dates
    subslice = tsrdd['2015-04-10':'2015-04-14']

    # Fill in missing values based on linear interpolation
    filled = subslice.fill('linear')
    

### DateTimeIndex

The time spanned by a TimeSeriesRDD is encapsulated in a `DateTimeIndex`, which is essentially an
ordered collection of timestamps.  DateTimeIndexes come in two flavors: uniform and irregular.
Uniform DateTimeIndexes have a concise representation including a start date, a frequency (i.e.
the interval between two timestamps), and a number of periods.  Irregular indices are simply
represented by an ordered collection of timestamps.
