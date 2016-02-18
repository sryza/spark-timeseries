[![Build Status](https://travis-ci.org/sryza/spark-timeseries.svg)](https://travis-ci.org/sryza/spark-timeseries)

Time Series for Spark (The `spark-ts` Package)
=============

A Scala / Java / Python library for interacting with time series data on Apache Spark.

Post questions and comments to the [Google group](https://groups.google.com/forum/#!forum/spark-ts), or email it directly at <mailto:spark-ts@googlegroups.com>.

Docs are available at http://sryza.github.io/spark-timeseries.

Or check out the [Scaladoc](http://sryza.github.io/spark-timeseries/0.2.0/scaladocs/index.html), [Javadoc](http://sryza.github.io/spark-timeseries/0.2.0/apidocs/index.html), or [Python doc](http://sryza.github.io/spark-timeseries/0.2.0/pydoc/py-modindex.html).

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
* [java.time](https://docs.oracle.com/javase/8/docs/api/index.html?java/time/package-summary.html) for dates and times. 
* [Apache Commons Math](https://commons.apache.org/proper/commons-math/) for general math and
  statistics functionality.
* [Apache Spark](https://spark.apache.org/) for distributed computation with in-memory
  capabilities.

Abstractions
-------------

The central abstraction of the library is the `TimeSeriesRDD`, a lazy distributed collection of univariate series with a conformed time dimension. It is lazy in the sense that it is an RDD: it encapsulates all the information needed to generate its elements, but doesn't materialize them upon instantiation. It is distributed in the sense that different univariate series within the collection can be stored and processed on different nodes.  Within each univariate series, observations are not distributed. The time dimension is conformed in the sense that a single `DateTimeIndex` applies to all the univariate series. Each univariate series within the RDD has a key to identify it. 

TimeSeriesRDDs then support efficient series-wise operations like slicing, imputing missing values
based on surrounding elements, and training time-series models.  In Scala:

    val tsRdd: TimeSeriesRDD = ...
    
    // Find a sub-slice between two dates 
    val subslice = tsRdd.slice(new DateTime("2015-4-10"), new DateTime("2015-4-14"))
    
    // Fill in missing values based on linear interpolation
    val filled = subslice.fill("linear")
    
    // Use an AR(1) model to remove serial correlations
    val residuals = filled.mapSeries(series => ar(series, 1).removeTimeDependentEffects(series))

In Python:

    tsrdd = ...
    
    # Find a sub-slice between two dates
    subslice = tsrdd['2015-04-10':'2015-04-14']
    
    # Fill in missing values based on linear interpolation
    filled = subslice.fill('linear')

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

Using this Repo
---------------

### Building

We use [Maven](https://maven.apache.org/) for building Java / Scala. To compile, run tests, and build
jars:

    mvn package

To run Python tests (requires [nose](https://nose.readthedocs.org/en/latest/)):

    cd python
    export SPARK_HOME=<location of local Spark installation>
    nosetests

### Running
    
To run a spark-shell with spark-ts and its dependencies on the classpath:

    spark-shell --jars target/sparkts-$VERSION-jar-with-dependencies.jar
    
### Releasing

To publish docs, easiest is to clone a separate version of this repo in some location we'll refer
to as DOCS_REPO.  Then:

    # Build main doc
    mvn site -Ddependency.locations.enabled=false
    
    # Build scaladoc
    mvn scala:doc
    
    # Build javadoc
    mvn javadoc:javadoc

    # Build Python doc
    cd python
    export SPARK_HOME=<location of local Spark installation>
    export PYTHONPATH=$PYTHONPATH::$SPARK_HOME/python:$SPARK_HOME/python/lib/*
    make html
    cd ..
    
    cp -r target/site/* $DOCS_REPO
    cp -r python/build/html/ $DOCS_REPO/pydoc
    cd $DOCS_REPO
    git checkout gh-pages
    git add -A
    git commit -m "Some message that includes the hash of the relevant commit in master"
    git push origin gh-pages

To build a Python source distribution, first build with Maven, then:

    cp target/sparkts-$VERSION-jar-with-dependencies.jar python/sparkts/
    cd python
    python setup.py sdist

To release Java/Scala packages (based on http://oryxproject.github.io/oryx/docs/how-to-release.html):

    mvn -Darguments="-DskipTests" -DreleaseVersion=$VERSION \
        -DdevelopmentVersion=$VERSION-SNAPSHOT release:prepare

    mvn -s private-settings.xml -Darguments="-DskipTests" release:perform

To release Python packages (based on http://peterdowns.com/posts/first-time-with-pypi.html):

    python setup.py register -r pypi
    python setup.py sdist upload -r pypi
