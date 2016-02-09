title: Overview

# Overview

Morlock (formerly Spark-TS) is a Python and Scala library for analyzing large-scale time series data
sets with Apache Spark.  It is hosted [here](https://github.com/cloudera/spark-timeseries).

Scaladoc is available [here](scaladocs/index.html).

Python doc is available [here](pydoc/py-modindex.html).

Morlock offers:

* A set of abstractions for manipulating time series data, similar to what's provided for smaller
  data sets in
  [Pandas](http://pandas.pydata.org/pandas-docs/dev/timeseries.html),
  [Matlab](http://www.mathworks.com/help/matlab/time-series.html), and R's
  [zoo](http://cran.r-project.org/web/packages/zoo/index.html) and
  [xts](http://cran.r-project.org/web/packages/xts/index.html) packages.
* Models, tests, and functions that enable dealing with time series from a statistical perspective,
  similar to what's provided in [StatsModels](http://statsmodels.sourceforge.net/devel/tsa.html)
  and a variety of Matlab and R packages.

The library is geared towards use cases in finance (munging tick data, building risk models), but
intends to be general enough that other fields with continuous time series data, like meteorology,
can make use of it.

The library currently expects that individual univariate time series can easily fit in memory on each
machine, but that collections of univariate time series may need to be distributed across many
machines. While time series that violate this expectation pose a bunch of fun distributed
programming problems, they don't tend to come up very often in finance, where an array holding
a value for every minute of every trading day for ten years needs less than a couple million
elements.

## Dependencies

The library sits on a few other excellent Java and Scala libraries.

* [Breeze](https://github.com/scalanlp/breeze) for NumPy-like, BLAS-able linear algebra.
* [JodaTime](http://www.joda.org/joda-time/) for dates and times. 
* [Apache Commons Math](https://commons.apache.org/proper/commons-math/) for general math and
  statistics functionality.
* [Apache Spark](https://spark.apache.org/) for distributed computation with in-memory
  capabilities.



## Functionality

### Time Series Manipulation

* Aligning
* Lagging
* Slicing by date-time
* Missing value imputation
* Conversion between different time series data layouts

### Time Series Math and Stats

* Exponentially weighted moving average (EWMA) models
* Autoregressive integrated moving average (ARIMA) models
* Generalized autoregressive conditional heteroskedastic (GARCH) models
* Missing data imputation
* Augmented Dickey-Fuller test
* Durbin-Watson test
* Breusch-Godfrey test
* Breusch-Pagan test
