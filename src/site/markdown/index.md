title: Overview

# Overview

Time Series for Spark (distributed as the `spark-ts` package) is a Scala / Java / Python library for
analyzing large-scale time series data sets.  It is hosted
[here](https://github.com/cloudera/spark-timeseries).

Post questions and comments to the [Google group](https://groups.google.com/forum/#!forum/spark-ts),
or email them directly to <mailto:spark-ts@googlegroups.com>.

Check out the [examples repository](https://github.com/sryza/spark-ts-examples) for a taste of what
it's like to use the library.

Check out the [Scaladoc](http://sryza.github.io/spark-timeseries/0.3.0/scaladocs/index.html),
[Javadoc](http://sryza.github.io/spark-timeseries/0.3.0/apidocs/index.html), or
[Python doc](http://sryza.github.io/spark-timeseries/0.3.0/pydoc/py-modindex.html).

Time Series for Spark offers:

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
