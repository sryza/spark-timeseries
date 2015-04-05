spark-timeseries
=============

A Scala / Java library for interacting with time series data on Apache Spark.

The aim here is to provide
* A set of abstractions for transforming and summarizing large time series data sets, similar to
  what's provided for smaller data sets in
  [Pandas](http://pandas.pydata.org/pandas-docs/dev/timeseries.html) and
  [Matlab](http://www.mathworks.com/help/matlab/time-series.html).
* Models, tests, and functions that enable dealing with time series from a statistical perspective,
  similar to what's provided in [StatsModels](http://statsmodels.sourceforge.net/devel/tsa.html)
  and a variety of Matlab and R packages.

The library is geared towards use cases in finance (munging tick data, building risk models), but
intends to be general enough that other fields with continuous time series data, like meteorology,
can make use of it.

The library expects that individual univariate time series can easily fit in memory on each
machine, but that collections of univariate time series may need to be distributed across many
machines. While time series that violate this expectation pose a bunch of fun distributed
programming problems, they don't tend to come up very often in finance, where an array holding
value for every minute of every trading day for ten years needs less than a couple million
elements.

The library sits on a few other excellent Java and Scala libraries.
* [Breeze](https://github.com/scalanlp/breeze) for NumPy-like, BLAS-able linear algebra.
* [JodaTime](http://www.joda.org/joda-time/) for dates and times. 
* [Apache Commons Math](https://commons.apache.org/proper/commons-math/) for general math and
  statistics functionality.
* [Apache Spark](https://spark.apache.org/) for distributed computation with in-memory
  capabilities.

Functionality
-------------

### Time Series

* Autoregressive models
* GARCH models
* Missing data imputation
* Augmented Dickey-Fuller test
* Durbin-Watson test

### General Prob / Stats

* Multivariate T Distribution

### Risk

Value at Risk (VaR) and Expected Shortfall (CVaR) through
* Monte Carlo simulation
* Bootstrapped historical simulation

Future Directions
-----------------

### Time Series

* Exponentially-weighted moving average
* EGARCH
