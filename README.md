[![Build Status](https://travis-ci.org/cloudera/spark-timeseries.svg)](https://travis-ci.org/cloudera/spark-timeseries)

spark-timeseries
=============

A Scala / Java library for interacting with time series data on Apache Spark.

Scaladoc is available at http://cloudera.github.io/spark-timeseries.

The aim here is to provide
* A set of abstractions for manipulating large time series data sets, similar to
  what's provided for smaller data sets in
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

The library sits on a few other excellent Java and Scala libraries.
* [Breeze](https://github.com/scalanlp/breeze) for NumPy-like, BLAS-able linear algebra.
* [JodaTime](http://www.joda.org/joda-time/) for dates and times. 
* [Apache Commons Math](https://commons.apache.org/proper/commons-math/) for general math and
  statistics functionality.
* [Apache Spark](https://spark.apache.org/) for distributed computation with in-memory
  capabilities.

Abstractions
-------------

The central abstraction of the library is the `TimeSeriesRDD`, a lazy distributed collection of univariate series with a conformed time dimension. It is lazy in the sense that it is an RDD: it encapsulates all the information needed to generate its elements, but doesn't materialize them upon instantiation. It is distributed in the sense that different univariate series within the collection can be stored and processed on different nodes.  Within each univariate series, observations are not distributed. The time dimension is conformed in the sense that a single `DateTimeIndex` applies to all the univariate series. Each univariate series within the RDD has a key to identify it. 

TimeSeriesRDDs then support efficient series-wise operations like slicing, imputing missing values based on surrounding elements, and training time-series models:

    val tsRdd: TimeSeriesRDD = ...
    
    // Find a sub-slice between two dates 
    val subslice = tsRdd.slice(new DateTime("2015-4-10"), new DateTime("2015-4-14"))
    
    // Fill in missing values based on linear interpolation
    val filled = subslice.fill("linear")
    
    // Use an AR(1) model to remove serial correlations
    val residuals = filled.mapSeries(series => ar(series, 1).removeTimeDependentEffects(series))


Functionality
--------------------------

### Time Series Manipulation
* Aligning
* Slicing by date-time
* Missing value imputation

### Time Series Math and Stats

* Exponentially weighted moving average (EWMA) models
* Autoregressive integrated moving average (ARIMA) models
* Generalized autoregressive conditional heteroskedastic (GARCH) models
* Missing data imputation
* Augmented Dickey-Fuller test
* Durbin-Watson test
* Breusch-Godfrey test
* Breusch-Pagan test

### General Prob / Stats

* Multivariate T Distribution

### Risk

Value at Risk (VaR) and Expected Shortfall (CVaR) through
* Monte Carlo simulation
* Bootstrapped historical simulation

Building
--------

We use [Maven](https://maven.apache.org/) for building Java / Scala. To compile, run tests, and build
jars:

    mvn package
    
To run a spark-shell with spark-timeseries and its dependencies on the classpath:

    spark-shell --jars target/sparktimeseries-0.0.1-jar-with-dependencies.jar

