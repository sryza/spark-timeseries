title: User Guide

# User Guide

## Terminology

A variety of terms are used to describe time series data, and many of these apply to conflicting or
overlapping concepts.  In the interest of clarity, we stick to the following set of definitions:

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

## Laying Out Time Series Data

The layout of a time-series data set determines what questions are easy to ask about it and how
quickly the answers to those questions can be computed. In Spark, we work with RDDs, DataFrames,
and Datasets, which are all collections of objects partitioned across a set of machines in the
cluster. Thus when choosing the layout of a data set, the most important question is
“What data do we store in each of these objects?”  Using "observations", "instants",
or "time series", as defined above, gives rise to three complementary layouts.

Most analysis starts out with taking data that has been compiled in a table of observations or
instants and formatting it into a table of time series.  The `spark-ts` package provides the
`TimeSeriesRDD` data structure for dealing with data in a time series layout, as well as
functionality for moving data into this layout from other layouts. 

Here's an example of data in an observations layout:

| Timestamp | Key  | Value |
|-----------|------|------|
|   5:00 PM | GOOG | $523 |
|   6:00 PM | GOOG |      |
|   7:00 PM | GOOG | $524 |
|   8:00 PM | GOOG | $600 |
|   5:00 PM | AAPL | $384 |
|   6:00 PM | AAPL | $384 |
|   7:00 PM | AAPL | $385 |
|   8:00 PM | AAPL | $385 |
|   5:00 PM | MSFT |  $40 |
|   6:00 PM | MSFT |  $60 |
|   7:00 PM | MSFT |      |
|   8:00 PM | MSFT |  $70 |

An advantage of this layout is that one can append data without needing the full vector of values at
a particular timestamp — each row only includes a single scalar value. There's also no need to worry
about changing the schema when one adds more keys..

However, the observations layout is not ideal for performing analysis. To ask most questions, one
needs to perform a group by on your data set, either by key or by timestamp, which is cumbersome as
well as computationally expensive.

Here's an example of data in an instants layout:

| Timestamp | GOOG | AAPL | MSFT |
|-----------|------|------|------|
|   5:00 PM | $523 | $384 |  $40 |
|   6:00 PM |      | $384 |  $60 |
|   7:00 PM | $524 | $385 |      |
|   8:00 PM | $600 | $385 |  $70 |

The “instants” layout is ideal for much of traditional machine learning — for example, building a
supervised learning model that predicts one variable based on contemporaneous values of the others.
However, it's not sufficient for operations that need to look at full series - for example imputing
missing values based on their temporal predecessors or fitting ARIMA models.

Ideal for this is the time series layout:

**Date-time index: [5:00 PM, 6:00 PM, 7:00 PM, 8:00 PM]**

|  Key |                   Series |
|------|--------------------------|
| GOOG | [$523, NaN, $524, $600]  | 
| AAPL | [$384, $384, $385, $385] | 
| MSFT | [$40, $60, NaN, $70]     | 

The date-time index defines a mapping of date-times to positions in arrays, and then each record
includes a time series represented as an array that conforms to this index.

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
    val zone = ZoneId.systemDefault()
    val subslice = tsRdd.slice(
      ZonedDateTime.of(LocalDateTime.parse("2015-04-10T00:00:00"), zone)
      ZonedDateTime.of(LocalDateTime.parse("2015-04-14T00:00:00"), zone))
    
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
