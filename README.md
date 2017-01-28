[![Build Status](https://travis-ci.org/sryza/spark-timeseries.svg)](https://travis-ci.org/sryza/spark-timeseries)

Time Series for Spark (The `spark-ts` Package)
=============

A Scala / Java / Python library for interacting with time series data on Apache Spark.

Post questions and comments to the [Google group](https://groups.google.com/forum/#!forum/spark-ts),
or email them directly to <mailto:spark-ts@googlegroups.com>.

### Note: The spark-ts library is no longer under active development by me (Sandy).  I unfortunately no longer have bandwidth to develop features, answer all questions on the mailing list, or fix all bugs that are filed.

### That said, I remain happy to review pull requests and do whatever I can to aid others in advancing the library.


Docs are available at http://sryza.github.io/spark-timeseries.

Or check out the [Scaladoc](http://sryza.github.io/spark-timeseries/0.3.0/scaladocs/index.html), [Javadoc](http://sryza.github.io/spark-timeseries/0.3.0/apidocs/index.html), or [Python doc](http://sryza.github.io/spark-timeseries/0.3.0/pydoc/py-modindex.html).

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

The library sits on a few other excellent Java and Scala libraries.

* [Breeze](https://github.com/scalanlp/breeze) for NumPy-like, BLAS-able linear algebra.
* [java.time](https://docs.oracle.com/javase/8/docs/api/index.html?java/time/package-summary.html) for dates and times. 
* [Apache Commons Math](https://commons.apache.org/proper/commons-math/) for general math and
  statistics functionality.
* [Apache Spark](https://spark.apache.org/) for distributed computation with in-memory
  capabilities.

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

    spark-shell --jars target/sparkts-$VERSION-SNAPSHOT-jar-with-dependencies.jar
    
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

    cp target/sparkts-$VERSION-SNAPSHOT-jar-with-dependencies.jar python/sparkts/
    cd python
    python setup.py sdist

To release Java/Scala packages (based on http://oryxproject.github.io/oryx/docs/how-to-release.html):

    mvn -Darguments="-DskipTests" -DreleaseVersion=$VERSION \
        -DdevelopmentVersion=$VERSION-SNAPSHOT release:prepare

    mvn -s private-settings.xml -Darguments="-DskipTests" release:perform

To release Python packages (based on http://peterdowns.com/posts/first-time-with-pypi.html):

    python setup.py register -r pypi
    python setup.py sdist upload -r pypi
